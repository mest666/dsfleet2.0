--[[
    dsfleet — hero_selection.lua

    Deploy to:  <dota install>/game/dota/scripts/vscripts/bots/hero_selection.lua
    (or, for a custom game, <addon>/scripts/vscripts/bots/hero_selection.lua)

    Responsibilities
      * Think()                 — pick heroes for every bot slot
      * UpdateLaneAssignments() — return the playerID -> lane map (the 2+1+2 split)
      * GetBotNames()           — stable, greppable names so server logs line up
                                  with dsfleet instance ids

    The lane map is the sanctioned equivalent of the client-side "instance index ->
    lane" scheme: the engine hands each bot its lane, and mode_laning_generic.lua
    reads it back via bot:GetAssignedLane(). No coordinate injection required.

    NOTE ON API DRIFT: the bot API changes between patches. Before relying on this,
    diff against the shipped reference scripts in
    game/dota/scripts/vscripts/bots/ and confirm the function names still match.
]]

------------------------------------------------------------------------------
-- Configuration
------------------------------------------------------------------------------

-- Slot order is stable within a team: index 1..5 maps to the team's five bot slots.
-- 2 + 1 + 2 => two top, one mid, two bot.
local LANE_LAYOUT = {
    LANE_TOP,   -- slot 1
    LANE_TOP,   -- slot 2
    LANE_MID,   -- slot 3
    LANE_BOT,   -- slot 4
    LANE_BOT,   -- slot 5
}

-- Heroes chosen for deterministic, low-variance behaviour: straightforward
-- last-hitting, no illusions or summons to inflate entity counts during netload runs.
local HERO_POOL = {
    [TEAM_RADIANT] = {
        "npc_dota_hero_sniper",
        "npc_dota_hero_lion",
        "npc_dota_hero_lina",
        "npc_dota_hero_dragon_knight",
        "npc_dota_hero_crystal_maiden",
    },
    [TEAM_DIRE] = {
        "npc_dota_hero_viper",
        "npc_dota_hero_shadow_shaman",
        "npc_dota_hero_zuus",
        "npc_dota_hero_bristleback",
        "npc_dota_hero_witch_doctor",
    },
}

local NAME_PREFIX = {
    [TEAM_RADIANT] = "dsfleet_R",
    [TEAM_DIRE]    = "dsfleet_D",
}

------------------------------------------------------------------------------
-- Internal state
------------------------------------------------------------------------------

local selected = {}          -- playerID -> true once SelectHero has been issued
local laneAssignments = nil  -- memoised playerID -> lane

------------------------------------------------------------------------------
-- Helpers
------------------------------------------------------------------------------

--- Returns the team's bot playerIDs in a stable, sorted order.
--  GetTeamPlayers ordering is not documented as stable, so sort explicitly:
--  an unstable order would silently reshuffle lanes between matches and make
--  run-to-run comparisons meaningless.
local function SortedBotPlayers(team)
    local ids = {}
    for _, id in pairs(GetTeamPlayers(team)) do
        if IsPlayerBot(id) then
            table.insert(ids, id)
        end
    end
    table.sort(ids)
    return ids
end

local function BuildLaneMapForTeam(team, out)
    local ids = SortedBotPlayers(team)
    for slot, playerID in ipairs(ids) do
        local lane = LANE_LAYOUT[slot] or LANE_MID
        out[playerID] = lane
    end
    return out
end

------------------------------------------------------------------------------
-- Engine entry points
------------------------------------------------------------------------------

--- Called every frame during hero selection until every bot has a hero.
function Think()
    for _, team in ipairs({ TEAM_RADIANT, TEAM_DIRE }) do
        local pool = HERO_POOL[team]
        local ids = SortedBotPlayers(team)
        for slot, playerID in ipairs(ids) do
            if not selected[playerID] then
                local hero = pool[slot] or pool[#pool]
                SelectHero(playerID, hero)
                selected[playerID] = true
                print(string.format(
                    "[dsfleet] team=%d slot=%d player=%d hero=%s lane=%d",
                    team, slot, playerID, hero, LANE_LAYOUT[slot] or LANE_MID))
            end
        end
    end
end

--- Returns playerID -> LANE_* for every bot. Called once by the engine.
function UpdateLaneAssignments()
    if laneAssignments ~= nil then
        return laneAssignments
    end
    local map = {}
    BuildLaneMapForTeam(TEAM_RADIANT, map)
    BuildLaneMapForTeam(TEAM_DIRE, map)
    laneAssignments = map

    local counts = { [LANE_TOP] = 0, [LANE_MID] = 0, [LANE_BOT] = 0 }
    for _, lane in pairs(map) do
        counts[lane] = (counts[lane] or 0) + 1
    end
    print(string.format("[dsfleet] lane distribution top=%d mid=%d bot=%d",
        counts[LANE_TOP], counts[LANE_MID], counts[LANE_BOT]))
    return map
end

--- Stable per-slot names. These land in `status` output over RCON, so dsfleet can
--- correlate a server-side bot with the instance that hosts it.
function GetBotNames()
    local names = {}
    for _, team in ipairs({ TEAM_RADIANT, TEAM_DIRE }) do
        local prefix = NAME_PREFIX[team]
        for slot = 1, 5 do
            local lane = LANE_LAYOUT[slot]
            local tag = (lane == LANE_TOP and "top")
                     or (lane == LANE_MID and "mid")
                     or "bot"
            table.insert(names, string.format("%s%d_%s", prefix, slot, tag))
        end
    end
    return names
end
