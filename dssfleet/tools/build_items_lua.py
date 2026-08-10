#!/usr/bin/env python3
"""Compile a JSON item-build profile into scripts/vscripts/bots/item_builds.lua.

    python tools/build_items_lua.py profiles/items.json \
        --out "D:/steamcmd/dota2ds/game/dota/scripts/vscripts/bots/item_builds.lua"

Input schema
------------
{
  "default": {
    "starting": ["item_tango", "item_branches"],
    "early":    ["item_boots"],
    "mid":      ["item_power_treads"],
    "late":     ["item_black_king_bar"]
  },
  "heroes": {
    "npc_dota_hero_sniper": { "starting": [...], "early": [...] }
  }
}

Keeping the build in JSON and generating Lua means the profile is diffable, validated
before it reaches the server, and hot-reloadable via `dota_bot_reload_scripts` over
RCON without editing Lua by hand.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Mapping

PHASES = ("starting", "early", "mid", "late")
ITEM_RE = re.compile(r"^item_[a-z0-9_]+$")
HERO_RE = re.compile(r"^npc_dota_hero_[a-z0-9_]+$")


class ProfileError(ValueError):
    pass


def _validate_phase_map(name: str, data: Any) -> dict[str, list[str]]:
    if not isinstance(data, Mapping):
        raise ProfileError(f"{name}: expected an object of phase -> item list")
    out: dict[str, list[str]] = {}
    for phase, items in data.items():
        if phase not in PHASES:
            raise ProfileError(f"{name}: unknown phase {phase!r} (expected one of {PHASES})")
        if not isinstance(items, list) or not all(isinstance(i, str) for i in items):
            raise ProfileError(f"{name}.{phase}: expected a list of item name strings")
        for item in items:
            if not ITEM_RE.match(item):
                raise ProfileError(
                    f"{name}.{phase}: {item!r} does not look like an item name "
                    f"(expected e.g. 'item_power_treads')")
        out[phase] = list(items)
    return out


def load_profile(path: Path) -> dict[str, Any]:
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ProfileError(f"cannot read {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ProfileError(f"invalid JSON in {path} (line {exc.lineno}): {exc.msg}") from exc
    if not isinstance(doc, Mapping):
        raise ProfileError("profile root must be an object")

    default = _validate_phase_map("default", doc.get("default", {}))
    if not default:
        raise ProfileError("profile must define a non-empty 'default' build")

    heroes: dict[str, dict[str, list[str]]] = {}
    for hero, build in (doc.get("heroes", {}) or {}).items():
        if not HERO_RE.match(hero):
            raise ProfileError(f"heroes: {hero!r} does not look like a hero unit name")
        heroes[hero] = _validate_phase_map(f"heroes.{hero}", build)

    return {"default": default, "heroes": heroes}


def _lua_string(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _emit_phase_map(build: Mapping[str, list[str]], indent: str) -> str:
    lines = []
    for phase in PHASES:
        items = build.get(phase)
        if not items:
            continue
        body = ", ".join(_lua_string(i) for i in items)
        lines.append(f"{indent}{phase} = {{ {body} }},")
    return "\n".join(lines)


def render_lua(profile: Mapping[str, Any], source: Path) -> str:
    out = [
        "-- GENERATED FILE — do not edit by hand.",
        f"-- Source: {source.name}",
        "-- Regenerate: python tools/build_items_lua.py <profile.json> --out <path>",
        "",
        "local M = {}",
        "",
        "M.default = {",
        _emit_phase_map(profile["default"], "    "),
        "}",
        "",
        "M.heroes = {",
    ]
    for hero, build in sorted(profile["heroes"].items()):
        out.append(f"    [{_lua_string(hero)}] = {{")
        out.append(_emit_phase_map(build, "        "))
        out.append("    },")
    out += ["}", "", "return M", ""]
    return "\n".join(line for line in out if line is not None)


def main() -> int:
    ap = argparse.ArgumentParser(prog="build_items_lua")
    ap.add_argument("profile", type=Path)
    ap.add_argument("--out", "-o", type=Path, required=True)
    args = ap.parse_args()

    try:
        profile = load_profile(args.profile)
    except ProfileError as exc:
        print(f"profile error: {exc}", file=sys.stderr)
        return 2

    lua = render_lua(profile, args.profile)
    try:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(lua, encoding="utf-8")
    except OSError as exc:
        print(f"cannot write {args.out}: {exc}", file=sys.stderr)
        return 1

    hero_count = len(profile["heroes"])
    item_count = sum(len(v) for v in profile["default"].values())
    print(f"wrote {args.out} — default build {item_count} items, "
          f"{hero_count} hero override(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
