#!/usr/bin/env python3
"""Mechanical layer check (spec STRATA-ARCH-01-001/002/003).

Reads the layer of every workspace crate from docs/architecture/layers.toml,
reads the crate dependency graph from `cargo metadata`, and fails on:
  - an upward dependency, or a skip-level one not listed under [[allow]];
  - any dependency on a composition root (`role = "root"`);
  - an edge listed under [[forbid]];
  - a workspace crate missing from the layer map, or a map entry, allow or
    forbid rule naming a crate or edge that does not exist (stale rules).

Usage:
  scripts/check_layers.py              check the workspace
  scripts/check_layers.py --self-test  run the checker's own tests
"""

from __future__ import annotations

import json
import subprocess
import sys
import tomllib
import unittest
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LAYERS = ROOT / "docs" / "architecture" / "layers.toml"


@dataclass(frozen=True)
class Dep:
    src: str
    dst: str
    kind: str  # "normal", "build" or "dev"


@dataclass(frozen=True)
class Crate:
    lo: int
    hi: int
    root: bool

    def label(self) -> str:
        if self.root:
            return "root"
        return f"L{self.lo}" if self.lo == self.hi else f"L{self.lo}-L{self.hi}"


def parse_crates(cfg: dict) -> tuple[dict[str, Crate], list[str]]:
    crates: dict[str, Crate] = {}
    errors: list[str] = []
    for name, spec in cfg.get("crates", {}).items():
        if spec.get("role") == "root":
            crates[name] = Crate(0, 0, True)
            continue
        layers = spec.get("layers")
        if (
            not isinstance(layers, list)
            or not layers
            or not all(isinstance(x, int) and 1 <= x <= 7 for x in layers)
            or sorted(layers) != list(range(min(layers), max(layers) + 1))
        ):
            errors.append(f"{name}: `layers` must be adjacent layers in 1..7, got {layers!r}")
            continue
        crates[name] = Crate(min(layers), max(layers), False)
    return crates, errors


def check(cfg: dict, members: set[str], deps: list[Dep]) -> list[str]:
    """Return every violation (empty = the graph satisfies the layer rules)."""
    crates, errors = parse_crates(cfg)
    for m in sorted(members - crates.keys()):
        errors.append(f"{m}: workspace crate has no layer in {LAYERS.name}")
    for c in sorted(crates.keys() - members):
        errors.append(f"{c}: listed in {LAYERS.name} but not a workspace crate")

    allow = {(a["from"], a["to"]): a.get("reason", "") for a in cfg.get("allow", [])}
    forbid: dict[tuple[str, str], str] = {}
    for f in cfg.get("forbid", []):
        targets = f["to"] if isinstance(f["to"], list) else [f["to"]]
        for t in targets:
            forbid[(f["from"], t)] = f.get("reason", "")
    for (a, b), reason in list(allow.items()) + list(forbid.items()):
        if not reason:
            errors.append(f"rule {a} -> {b}: a reason citing the spec is required")
        for c in (a, b):
            if c not in crates:
                errors.append(f"rule {a} -> {b}: unknown crate {c}")

    edges = {(d.src, d.dst) for d in deps if d.kind != "dev"}
    for e in sorted(allow.keys() - edges):
        errors.append(f"allow {e[0]} -> {e[1]}: no such dependency (stale rule, remove it)")

    for d in sorted(deps, key=lambda d: (d.src, d.dst, d.kind)):
        s, t = crates.get(d.src), crates.get(d.dst)
        if s is None or t is None:
            continue  # reported above
        edge = f"{d.src} ({s.label()}) -> {d.dst} ({t.label()})"
        if d.kind == "dev":
            edge += " [dev]"
        if (d.src, d.dst) in forbid:
            errors.append(f"{edge}: forbidden: {forbid[(d.src, d.dst)]}")
            continue
        if t.root:
            if not s.root:
                errors.append(f"{edge}: nothing may depend on a composition root")
            continue
        if s.root:
            continue
        if t.hi > s.hi:
            errors.append(f"{edge}: upward dependency (ARCH-01-001)")
            continue
        if d.kind == "dev":
            continue  # tests may skip layers
        adjacent = t.hi == 1 or t.hi >= s.lo - 1
        if not adjacent and (d.src, d.dst) not in allow:
            errors.append(
                f"{edge}: skips a layer (ARCH-01-001: only the layer directly below and L1); "
                f"route it through the layer between or add a justified [[allow]]"
            )
    return errors


def workspace_graph() -> tuple[set[str], list[Dep]]:
    out = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    meta = json.loads(out)
    members = {p["name"] for p in meta["packages"]}
    deps = []
    for p in meta["packages"]:
        for d in p["dependencies"]:
            if d["name"] in members:
                deps.append(Dep(p["name"], d["name"], d.get("kind") or "normal"))
    return members, list(dict.fromkeys(deps))


def main() -> int:
    if "--self-test" in sys.argv[1:]:
        result = unittest.main(argv=[sys.argv[0]], exit=False, verbosity=1).result
        return 0 if result.wasSuccessful() else 1
    cfg = tomllib.loads(LAYERS.read_text(encoding="utf-8"))
    members, deps = workspace_graph()
    errors = check(cfg, members, deps)
    if errors:
        print(f"layer check FAILED ({len(errors)} violation(s)):", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        return 1
    crates, _ = parse_crates(cfg)
    normal = [d for d in deps if d.kind != "dev"]
    allowed = [d for d in normal if (d.src, d.dst) in {(a["from"], a["to"]) for a in cfg.get("allow", [])}]
    print(
        f"layer check: {len(crates)} crates, {len(normal)} dependency edges "
        f"(+{len(deps) - len(normal)} dev), {len(allowed)} sanctioned exception(s), 0 violations"
    )
    return 0


# --------------------------------------------------------------------------
# Self-tests: a small layered fixture, then one deliberate violation each.

BASE = {
    "crates": {
        "common": {"layers": [1]},
        "storage": {"layers": [2]},
        "format": {"layers": [2]},
        "query": {"layers": [3, 4]},
        "server": {"layers": [5, 6]},
        "sdk": {"layers": [7]},
        "bench": {"role": "root"},
    },
    "allow": [{"from": "server", "to": "storage", "reason": "IX.1.4"}],
    "forbid": [{"from": "query", "to": ["format"], "reason": "ARCH-01-003"}],
}
MEMBERS = set(BASE["crates"])
GOOD = [
    Dep("storage", "common", "normal"),
    Dep("storage", "format", "normal"),
    Dep("query", "storage", "normal"),
    Dep("query", "common", "normal"),
    Dep("server", "query", "normal"),
    Dep("server", "storage", "normal"),
    Dep("sdk", "server", "dev"),
    Dep("sdk", "storage", "dev"),
    Dep("bench", "server", "normal"),
    Dep("bench", "format", "normal"),
]


class LayerRules(unittest.TestCase):
    def errs(self, extra=(), cfg=None, members=None, deps=None):
        deps = list(GOOD if deps is None else deps) + list(extra)
        return check(cfg or BASE, MEMBERS if members is None else members, deps)

    def test_clean_graph_passes(self):
        self.assertEqual(self.errs(), [])

    def test_upward_dependency_fails(self):
        e = self.errs([Dep("storage", "query", "normal")])
        self.assertEqual(len(e), 1)
        self.assertIn("upward", e[0])

    def test_upward_dev_dependency_fails(self):
        e = self.errs([Dep("common", "storage", "dev")])
        self.assertEqual(len(e), 1)
        self.assertIn("upward", e[0])

    def test_skip_level_fails_unless_allowed(self):
        e = self.errs([Dep("sdk", "query", "normal")])
        self.assertEqual(len(e), 1)
        self.assertIn("skips a layer", e[0])
        cfg = dict(BASE, allow=[])
        e = self.errs(cfg=cfg)
        self.assertEqual(len(e), 1)
        self.assertIn("server (L5-L6) -> storage (L2)", e[0])

    def test_dev_dependency_may_skip_layers(self):
        self.assertEqual(self.errs([Dep("server", "common", "dev")]), [])

    def test_forbidden_edge_fails(self):
        e = self.errs([Dep("query", "format", "normal")])
        self.assertEqual(len(e), 1)
        self.assertIn("ARCH-01-003", e[0])

    def test_depending_on_a_root_fails(self):
        e = self.errs([Dep("server", "bench", "dev")])
        self.assertEqual(len(e), 1)
        self.assertIn("composition root", e[0])

    def test_unlisted_and_stale_entries_fail(self):
        e = self.errs(members=MEMBERS | {"newcrate"})
        self.assertTrue(any("newcrate: workspace crate has no layer" in x for x in e))
        e = self.errs(deps=[d for d in GOOD if (d.src, d.dst) != ("server", "storage")])
        self.assertTrue(any("stale rule" in x for x in e))

    def test_malformed_layers_fail(self):
        cfg = dict(BASE, crates=dict(BASE["crates"], query={"layers": [2, 4]}))
        e = self.errs(cfg=cfg)
        self.assertTrue(any("adjacent layers" in x for x in e))

    def test_rules_need_a_reason(self):
        cfg = dict(BASE, forbid=[{"from": "query", "to": "format"}])
        e = self.errs(cfg=cfg)
        self.assertTrue(any("reason" in x for x in e))


if __name__ == "__main__":
    sys.exit(main())
