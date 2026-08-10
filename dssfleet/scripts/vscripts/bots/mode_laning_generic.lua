--[[
    dsfleet — mode_laning_generic.lua

    Deploy to: <dota install>/game/dota/scripts/vscripts/bots/mode_laning_generic.lua

    A deterministic laning state machine. Each bot walks its assigned lane's waypoint
    chain to the lane equilibrium point, holds position, and last-hits. The point is
    reproducibility: identical inputs must produce identical network traffic across
    runs, otherwise A/B netload comparisons are noise.

    State machine
        DEPLOY   -> walking the waypoint chain out of the fountain
        HOLD     -> at the equilibrium point, last-hitting
        RETREAT  -> below health threshold, walking back toward the tower
        RESET    -> dead / respawning

    Telemetry
        Emits a compact line to the server console every TELEMETRY_INTERVAL seconds.
        dsfleet's log pump captures it, so per-bot GPM/XPM/state shows up in the
        instance log without any extra plumbing.
]]

------------------------------------------------------------------------------
-- Waypoints
------------------------------------------------------------------------------

-- Approximate Dota map coordinates. The map spans roughly -8000..8000 on both axes;
-- Radiant fountain sits near (-7100, -6700), Dire near (7000, 6500).
--
-- TUNE THESE. They are close enough that bots path sensibly, but the exact
-- equilibrium points shift with each map revision. Verify in-game with
-- `dota_camera_get_lookatpos` or by printing bot:GetLocation() before trusting them
-- for measurement runs.
local WAYPOINTS = {
    [TEAM_RADIANT] = {
        [LANE_TOP] = {
            Vector(-6700, -5500, 128),
            Vector(-6800, -1500, 128),
            Vector(-6600,  2200, 128),
            Vector(-5900,  4900, 128),   -- equilibrium
        },
        [LANE_MID] = {
            Vector(-5300, -5000, 128),
            Vector(-3400, -3100, 128),
            Vector(-1400, -1100, 128),
            Vector(  -300,  -200, 128),  -- equilibrium
        },
        [LANE_BOT] = {
            Vector(-5000, -6300, 128),
            Vector(-1200, -6400, 128),
            Vector( 2400, -6300, 128),
            Vector( 4700, -6100, 128),   -- equilibrium
        },
    },
    [TEAM_DIRE] = {
        [LANE_TOP] = {
            Vector( 4900,  6100, 128),
            Vector( 1200,  6300, 128),
            Vector(-2200,  6300, 128),
            Vector(-4700,  6000, 128),   -- equilibrium
        },
        [LANE_MID] = {
            Vector( 5100,  4800, 128),
            Vector( 3300,  3000, 128),
            Vector( 1300,  1000, 128),
            Vector(   300,   200, 128),  -- equilibrium
        },
        [LANE_BOT] = {
            Vector( 6400,  5000, 128),
            Vector( 6500,  1200, 128),
            Vector( 6300, -2300, 128),
            Vector( 6000, -4800, 128),   -- equilibrium
        },
    },
}

------------------------------------------------------------------------------
-- Tunables
------------------------------------------------------------------------------

local ARRIVAL_RADIUS      = 300      -- distance at which a waypoint counts as reached
local RETREAT_HEALTH_PCT  = 0.35     -- below this fraction, fall back
local RESUME_HEALTH_PCT   = 0.70     -- above this fraction, push out again
local HOLD_JITTER         = 120      -- small positional jitter so bots don't stack
local TELEMETRY_INTERVAL  = 15.0     -- seconds between console telemetry lines
local ATTACK_INTERVAL     = 0.35     -- minimum seconds between attack orders

local STATE_DEPLOY, STATE_HOLD, STATE_RETREAT, STATE_RESET = 1, 2, 3, 4
local STATE_NAMES = { "deploy", "hold", "retreat", "reset" }

------------------------------------------------------------------------------
-- Per-bot state (keyed by playerID; bot scripts run one VM per bot, but keying
-- defensively costs nothing and survives any future shared-VM change)
------------------------------------------------------------------------------

local state = {}

local function BotState(bot)
    local id = bot:GetPlayerID()
    if state[id] == nil then
        state[id] = {
            phase          = STATE_DEPLOY,
            waypointIndex  = 1,
            lastTelemetry  = 0,
            lastAttack     = 0,
            deployedAt     = nil,
        }
    end
    return state[id]
end

local function LaneWaypoints(bot)
    local team = bot:GetTeam()
    local lane = bot:GetAssignedLane()
    local byTeam = WAYPOINTS[team]
    if byTeam == nil then return nil end
    return byTeam[lane] or byTeam[LANE_MID]
end

local function HealthFraction(bot)
    local maxHealth = bot:GetMaxHealth()
    if maxHealth == nil or maxHealth <= 0 then return 1.0 end
    return bot:GetHealth() / maxHealth
end

local function Jitter(bot, vec)
    -- Deterministic per-player offset: stacked bots produce degenerate collision
    -- traffic that pollutes netload measurements.
    local id = bot:GetPlayerID()
    local dx = ((id * 37) % 7 - 3) / 3 * HOLD_JITTER
    local dy = ((id * 53) % 7 - 3) / 3 * HOLD_JITTER
    return Vector(vec.x + dx, vec.y + dy, vec.z)
end

------------------------------------------------------------------------------
-- Telemetry
------------------------------------------------------------------------------

local function EmitTelemetry(bot, st)
    local now = DotaTime()
    if now - st.lastTelemetry < TELEMETRY_INTERVAL then return end
    st.lastTelemetry = now

    -- Single greppable line. dsfleet's log pump writes this straight to
    -- <runtime_dir>/logs/<instance>.log.
    print(string.format(
        "[dsfleet.bot] t=%.1f player=%d team=%d lane=%d state=%s gpm=%d xpm=%d " ..
        "lh=%d dn=%d gold=%d lvl=%d hp=%.2f",
        now,
        bot:GetPlayerID(),
        bot:GetTeam(),
        bot:GetAssignedLane(),
        STATE_NAMES[st.phase] or "?",
        bot:GetGoldPerMinute() or 0,
        bot:GetXPPerMinute() or 0,
        bot:GetLastHits() or 0,
        bot:GetDenies() or 0,
        bot:GetGold() or 0,
        bot:GetLevel() or 0,
        HealthFraction(bot)))
end

------------------------------------------------------------------------------
-- Last hitting
------------------------------------------------------------------------------

--- Attacks a creep only when the hit will actually kill it. Blindly auto-attacking
--- pushes the lane, which drags the equilibrium point and desynchronises the run.
local function TryLastHit(bot, st)
    local now = DotaTime()
    if now - st.lastAttack < ATTACK_INTERVAL then return false end

    local damage = bot:GetAttackDamage()
    local enemies = bot:GetNearbyLaneCreeps(700, true)
    for _, creep in pairs(enemies or {}) do
        if creep:IsAlive() and creep:GetHealth() <= damage then
            bot:Action_AttackUnit(creep, true)
            st.lastAttack = now
            return true
        end
    end

    -- Deny our own creep when it is about to die anyway.
    local allies = bot:GetNearbyLaneCreeps(700, false)
    for _, creep in pairs(allies or {}) do
        if creep:IsAlive()
           and creep:GetHealth() <= damage
           and creep:GetHealth() / creep:GetMaxHealth() < 0.5 then
            bot:Action_AttackUnit(creep, true)
            st.lastAttack = now
            return true
        end
    end
    return false
end

------------------------------------------------------------------------------
-- Engine entry points
------------------------------------------------------------------------------

function GetDesire()
    local bot = GetBot()
    if bot == nil or not bot:IsAlive() then
        return BOT_MODE_DESIRE_NONE
    end
    -- Laning stays dominant for the whole measurement window; this framework is for
    -- reproducible load, not for winning games.
    return BOT_MODE_DESIRE_HIGH
end

function OnStart()
    local bot = GetBot()
    if bot == nil then return end
    local st = BotState(bot)
    st.phase = STATE_DEPLOY
    st.waypointIndex = 1
    st.deployedAt = DotaTime()
    print(string.format("[dsfleet.bot] player=%d entering laning lane=%d",
        bot:GetPlayerID(), bot:GetAssignedLane()))
end

function OnEnd()
    local bot = GetBot()
    if bot == nil then return end
    local st = BotState(bot)
    st.phase = STATE_RESET
end

function Think()
    local bot = GetBot()
    if bot == nil then return end

    local st = BotState(bot)

    if not bot:IsAlive() then
        st.phase = STATE_RESET
        st.waypointIndex = 1
        return
    end
    if st.phase == STATE_RESET then
        st.phase = STATE_DEPLOY
    end

    EmitTelemetry(bot, st)

    local waypoints = LaneWaypoints(bot)
    if waypoints == nil or #waypoints == 0 then
        return  -- no route for this team/lane; leave the bot to default behaviour
    end

    local health = HealthFraction(bot)

    -- ---- retreat / resume -------------------------------------------------
    if st.phase ~= STATE_RETREAT and health < RETREAT_HEALTH_PCT then
        st.phase = STATE_RETREAT
    elseif st.phase == STATE_RETREAT and health >= RESUME_HEALTH_PCT then
        st.phase = STATE_DEPLOY
    end

    if st.phase == STATE_RETREAT then
        -- Fall back one waypoint toward the fountain.
        local target = waypoints[math.max(1, st.waypointIndex - 1)]
        bot:Action_MoveToLocation(target)
        return
    end

    -- ---- deploy -----------------------------------------------------------
    if st.phase == STATE_DEPLOY then
        local target = waypoints[st.waypointIndex]
        local distance = GetUnitToLocationDistance(bot, target)
        if distance < ARRIVAL_RADIUS then
            if st.waypointIndex >= #waypoints then
                st.phase = STATE_HOLD
                print(string.format("[dsfleet.bot] player=%d reached equilibrium after %.1fs",
                    bot:GetPlayerID(), DotaTime() - (st.deployedAt or DotaTime())))
            else
                st.waypointIndex = st.waypointIndex + 1
            end
        else
            bot:Action_MoveToLocation(target)
        end
        return
    end

    -- ---- hold -------------------------------------------------------------
    if st.phase == STATE_HOLD then
        if TryLastHit(bot, st) then
            return
        end
        local anchor = Jitter(bot, waypoints[#waypoints])
        if GetUnitToLocationDistance(bot, anchor) > ARRIVAL_RADIUS then
            bot:Action_MoveToLocation(anchor)
        end
    end
end
