--[[
    dsfleet — item_purchase_generic.lua

    Deploy to: <dota install>/game/dota/scripts/vscripts/bots/item_purchase_generic.lua

    Purchases a fixed, declarative build order. Deterministic buys matter for the same
    reason deterministic laning does: item timings change movement speed, kill timings
    and therefore the network traffic profile. A randomised build makes two runs
    incomparable.

    Build source
      Reads scripts/vscripts/bots/item_builds.lua, which dsfleet generates from the
      JSON profile (see tools/build_items_lua.py). Falls back to DEFAULT_BUILD if that
      file is absent, so a fresh checkout still runs.

    The engine calls ItemPurchaseThink() on a timer; there is no shop UI interaction
    and no courier hotkey — ActionImmediate_PurchaseItem handles the transaction
    server-side, and the engine's own courier logic delivers.
]]

------------------------------------------------------------------------------
-- Build data
------------------------------------------------------------------------------

local DEFAULT_BUILD = {
    starting = {
        "item_tango",
        "item_branches",
        "item_branches",
        "item_circlet",
    },
    early = {
        "item_magic_wand",
        "item_boots",
        "item_wraith_band",
    },
    mid = {
        "item_power_treads",
        "item_magic_wand",
        "item_falcon_blade",
    },
    late = {
        "item_black_king_bar",
        "item_desolator",
    },
}

-- Optional per-hero overrides, keyed by unit name.
local HERO_BUILDS = {}

local ok, generated = pcall(require, "bots.item_builds")
if ok and type(generated) == "table" then
    if type(generated.default) == "table" then
        DEFAULT_BUILD = generated.default
    end
    if type(generated.heroes) == "table" then
        HERO_BUILDS = generated.heroes
    end
    print("[dsfleet] loaded generated item builds")
else
    print("[dsfleet] item_builds.lua not found; using compiled-in default build")
end

------------------------------------------------------------------------------
-- Tunables
------------------------------------------------------------------------------

local PHASE_ORDER      = { "starting", "early", "mid", "late" }
local RESERVE_GOLD     = 0        -- keep this much unspent (e.g. for buyback tests)
local PURCHASE_COOLDOWN = 1.0     -- seconds between purchase attempts

------------------------------------------------------------------------------
-- State
------------------------------------------------------------------------------

local purchaseState = {}

local function BuildFor(bot)
    local unitName = bot:GetUnitName()
    return HERO_BUILDS[unitName] or DEFAULT_BUILD
end

--- Flattens the phase tables into one ordered queue on first use.
local function PurchaseQueue(bot)
    local id = bot:GetPlayerID()
    if purchaseState[id] == nil then
        local build = BuildFor(bot)
        local queue = {}
        for _, phase in ipairs(PHASE_ORDER) do
            for _, item in ipairs(build[phase] or {}) do
                table.insert(queue, { name = item, phase = phase })
            end
        end
        purchaseState[id] = {
            queue     = queue,
            index     = 1,
            lastTry   = 0,
            purchased = 0,
        }
        print(string.format("[dsfleet] player=%d build queued: %d items (%s)",
            id, #queue, bot:GetUnitName()))
    end
    return purchaseState[id]
end

------------------------------------------------------------------------------
-- Engine entry point
------------------------------------------------------------------------------

function ItemPurchaseThink()
    local bot = GetBot()
    if bot == nil or not bot:IsAlive() then return end

    local st = PurchaseQueue(bot)
    if st.index > #st.queue then
        return  -- build complete
    end

    local now = DotaTime()
    if now - st.lastTry < PURCHASE_COOLDOWN then return end
    st.lastTry = now

    local entry = st.queue[st.index]
    local cost = GetItemCost(entry.name)

    if cost == nil or cost <= 0 then
        -- Unknown item name: skip it rather than stalling the whole queue forever.
        print(string.format("[dsfleet] player=%d unknown item %s — skipping",
            bot:GetPlayerID(), entry.name))
        st.index = st.index + 1
        return
    end

    -- Tell the engine what we are saving for so it stops auto-spending.
    bot:SetNextItemPurchaseValue(cost)

    if bot:GetGold() - RESERVE_GOLD < cost then
        return  -- keep saving
    end

    bot:ActionImmediate_PurchaseItem(entry.name)
    st.index = st.index + 1
    st.purchased = st.purchased + 1

    print(string.format("[dsfleet.buy] t=%.1f player=%d item=%s phase=%s cost=%d gold_after=%d",
        now, bot:GetPlayerID(), entry.name, entry.phase, cost, bot:GetGold()))

    if st.index > #st.queue then
        bot:SetNextItemPurchaseValue(0)
        print(string.format("[dsfleet] player=%d build complete (%d items)",
            bot:GetPlayerID(), st.purchased))
    end
end
