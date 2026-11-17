#!/usr/bin/env python3
"""The round config of the load test: read a TOML file, merge it over the built-in defaults, apply the
ALAMBIQUE_LOAD_* overrides, validate it, and expand it into cases.

  load_config.py --check FILE      print the resolved config (JSON) and its hash; exit 1 and list every problem when invalid
  load_config.py --dry-run FILE    print what the round would run, step by step, without starting anything
  load_config.py --prepare FILE    validate, write config.json and one case.json per case into the results directory, print
                                   one "scenario measure variant case.json" line per case (what scripts/load runs)
  load_config.py --shell CASE      print the settings scripts/load-case needs as shell assignments
  load_config.py --service-env CASE  print the environment file of the service container
  load_config.py --defaults        print the built-in defaults

The defaults are today's defaults of the load test; load/rounds/example.toml lists every key with its default and a comment.
As a module: load_config(path, env) -> dict, resolve(raw, env) -> dict, cases(config) -> list, case_config(config, scenario,
measure, variant) -> dict, levels(steps) -> list, config_hash(case) -> str. ConfigError.problems lists every problem found.
"""
import argparse
import copy
import datetime
import difflib
import hashlib
import json
import math
import os
import re
import shlex
import sys
import tomllib

SCENARIOS = ("A", "B")
MEASURES = ("commits", "cells", "wakes", "query")
NAME = re.compile(r"^[a-z][a-z0-9_]*$")
SIZE = re.compile(r"^([0-9]+)([kmg]?)b?$", re.IGNORECASE)
MIB = 1 << 20
UNITS = {"": 1, "k": 1 << 10, "m": MIB, "g": 1 << 30}
SKIP_HASH = ("host", "levels", "hash", "dir", "command", "skip_done", "timeouts", "wait")


class ConfigError(Exception):
    """An invalid config; `problems` lists every problem found, as "path: message"."""

    def __init__(self, problems, source=None):
        self.problems, self.source = list(problems), source
        super().__init__(f"invalid load config{f' {source}' if source else ''}:\n" + "\n".join(f"  - {p}" for p in self.problems))


# ---- the schema ---------------------------------------------------------------------------------------

class Leaf:
    """One value: `kind` int, float, bool, str, size or list; `lo`/`hi` bound a number (`open_lo`: lo itself is out);
    `choices` restrict a str or the items of a list; `item` is the kind of the items of a list."""

    def __init__(self, kind, default, lo=None, hi=None, open_lo=False, choices=None, item=None, nonempty=False, ascending=False):
        self.kind, self.default, self.lo, self.hi, self.open_lo = kind, default, lo, hi, open_lo
        self.choices, self.item, self.nonempty, self.ascending = choices, item, nonempty, ascending

    def range_text(self):
        parts = []
        if self.lo is not None:
            parts.append(f"{'>' if self.open_lo else '>='} {self.lo:g}")
        if self.hi is not None:
            parts.append(f"<= {self.hi:g}")
        return " and ".join(parts)

    def number(self, path, value, kind, problems):
        wrong = isinstance(value, bool) or not isinstance(value, (int, float) if kind == "float" else int)
        if wrong or (isinstance(value, float) and not math.isfinite(value)):
            problems.append(f"{path}: expected {'a number' if kind == 'float' else 'a whole number'}, got {describe(value)}")
            return None
        low = self.lo is not None and (value <= self.lo if self.open_lo else value < self.lo)
        if low or (self.hi is not None and value > self.hi):
            problems.append(f"{path}: {value:g} is out of range, expected {self.range_text()}")
            return None
        return float(value) if kind == "float" else value

    def check(self, path, value, problems):
        """The normalised value (a float leaf holds floats, so 1 and 1.0 are one value), or None after a problem."""
        if self.kind in ("int", "float"):
            return self.number(path, value, self.kind, problems)
        if self.kind == "bool":
            if not isinstance(value, bool):
                problems.append(f"{path}: expected true or false, got {describe(value)}")
                return None
            return value
        if self.kind in ("str", "size"):
            if not isinstance(value, str):
                problems.append(f"{path}: expected a string, got {describe(value)}")
                return None
            if self.kind == "size" and size_mb(value) is None:
                problems.append(f"{path}: {value!r} is not a size, use a whole number of at least 1 MiB with k, m or g (512m, 2g)")
                return None
            if self.choices and value not in self.choices:
                problems.append(f"{path}: {value!r} is not one of {', '.join(self.choices)}")
                return None
            return value
        return self.check_list(path, value, problems)

    def check_list(self, path, value, problems):
        if not isinstance(value, list):
            problems.append(f"{path}: expected a list, got {describe(value)}")
            return None
        if self.nonempty and not value:
            problems.append(f"{path}: must not be empty")
            return None
        item = Leaf(self.item, None, self.lo, self.hi, self.open_lo, self.choices)
        found = [item.check(f"{path}[{i}]", v, problems) for i, v in enumerate(value)]
        if any(v is None for v in found):
            return None
        if self.ascending and any(b <= a for a, b in zip(found, found[1:])):
            problems.append(f"{path}: the values must grow, each above the one before")
            return None
        if self.choices and len(set(found)) != len(found):
            problems.append(f"{path}: lists a value twice")
            return None
        return found


class Table:
    """A table of known keys. `sparse`: only the keys that are given are kept (an override of another table), on top of
    `default` (a dict, {} when absent)."""

    def __init__(self, children, sparse=False, default=None):
        self.children, self.sparse, self.default = children, sparse, default or {}


class Map:
    """A table of named entries, each holding `child(name)`: `keys` lists the names allowed (None: any name matching NAME);
    the entries of `defaults` (a dict of name to value) are always present."""

    def __init__(self, keys, child, label, defaults=None):
        self.keys, self.child, self.label, self.defaults = keys, child, label, defaults or {}


def describe(value):
    return {bool: "a boolean", int: "a whole number", float: "a number", str: "a string", list: "a list", dict: "a table"}.get(type(value), type(value).__name__)


def size_mb(text):
    """The size of a docker size string such as 512m, 2g or 768M (k, m, g; a bare number is bytes) in MiB, or None."""
    found = SIZE.match(text) if isinstance(text, str) else None
    if not found:
        return None
    mb = int(found.group(1)) * UNITS[found.group(2).lower()] / MIB
    return int(mb) if mb >= 1 and mb == int(mb) else None


def positive(default):
    return Leaf("float", float(default), lo=0, open_lo=True)


def pct(default, lo=0, open_lo=True):
    return Leaf("float", float(default), lo=lo, hi=100, open_lo=open_lo)


def criterion(sparse=False, **limits):
    """A criterion: `enabled` and its limits."""
    return Table({"enabled": Leaf("bool", True), **limits}, sparse)


def judging_children(sparse):
    return {
        "latency": criterion(sparse, percentile=Leaf("str", "p99", choices=("p50", "p95", "p99")), max_ms=positive(250)),
        "memory": criterion(sparse, max_pct=pct(100)),
        "errors": criterion(sparse, max_pct=pct(0, open_lo=False)),
        "busy": criterion(sparse, max_pct=pct(1, open_lo=False)),
        "delivery": criterion(sparse, min_ratio=Leaf("float", 0.95, lo=0, hi=1, open_lo=True)),
        "store_full": criterion(sparse, max_pct=pct(95), error_pct=pct(85), min_free_mb=Leaf("float", 1024.0, lo=0)),
        "data_full": criterion(sparse, estimate_pct=pct(90), full_pct=pct(95)),
        "driver_swap": criterion(sparse, max_mb=Leaf("float", 16.0, lo=0)),
        "store_swap": criterion(sparse, max_growth_mb=Leaf("float", 0.0, lo=0)),
        "host_swap": criterion(sparse, max_mb=Leaf("float", 512.0, lo=0)),
        "host_load": criterion(sparse, max=positive(6), quiet_window_s=positive(30), quiet_max_wait_s=positive(1800)),
        "host_memory": criterion(sparse, min_available_gib=Leaf("float", 6.0, lo=0)),
        "host_start": criterion(sparse, max_load=positive(8), max_wait_s=Leaf("float", 600.0, lo=0)),
        "cooldown": criterion(sparse, max_mem_pct=pct(80), max_wait_s=positive(300)),
        "suspect_throttle": criterion(sparse, max_pct=pct(10)),
    }


def steps_table(start, maximum, refine=2, confirm=True, sparse=False, default=None):
    """The step plan of a measure: a `ladder` of fixed values, or a `bisect` that grows from `start` by `factor` up to `max`,
    then refines by bisection after the first bad step."""
    return Table({
        "kind": Leaf("str", "bisect", choices=("ladder", "bisect")),
        "values": Leaf("list", [], item="int", lo=0, open_lo=True, ascending=True),
        "start": Leaf("float", float(start), lo=0, open_lo=True),
        "factor": Leaf("float", 2.0, lo=1, open_lo=True),
        "max": Leaf("float", float(maximum), lo=0, open_lo=True),
        "duration_s": Leaf("float", 180.0, lo=0, open_lo=True),
        "refine": Leaf("int", refine, lo=0),
        "confirm": Leaf("bool", confirm),
    }, sparse, default)


STORE_LEAVES = {
    "backing": Leaf("str", "tmpfs", choices=("tmpfs", "disk")),
    "tmpfs_size": Leaf("size", "4g"),
    "dir": Leaf("str", ""),
    "latency_ms": Leaf("float", 25.0, lo=0),
    "image": Leaf("str", "chrislusf/seaweedfs:4.48"),
}
SERVICE_LEAVES = {
    "image": Leaf("str", "alambique-load:local"),
    "erl_aflags": Leaf("str", "+Mdai max +MMmcs 0"),
    "wal_checkpoint_bytes": Leaf("int", 8388608, lo=1),
    "idle_timeout_ms": Leaf("int", 0, lo=0),
    "max_cells": Leaf("int", 0, lo=0),
    "cache_kib": Leaf("int", 0, lo=0),
    "opts": Leaf("str", ""),
}
CELL_DATA = {"cell_rows": Leaf("int", 20, lo=1), "row_bytes": Leaf("int", 1500, lo=1), "populate_concurrency": Leaf("int", 64, lo=1)}
WORKLOADS = {
    "commits": {"cells": Leaf("int", 64, lo=1), "rows": Leaf("int", 8, lo=1)},
    "cells": {"fill_rate": Leaf("int", 100, lo=1), "hold_s": Leaf("float", 60.0, lo=0)},
    "wakes": {"mode": Leaf("str", "cold", choices=("cold", "local")), "tail_s": Leaf("float", 60.0, lo=0),
              "cell_bytes": Leaf("int", 160000, lo=1), **CELL_DATA},
    "query": {"population": Leaf("int", 200, lo=1), "cells_per_query": Leaf("int", 20, lo=1), **CELL_DATA},
}
RAMPS = {"commits": (50, 3200, 2, True), "cells": (500, 32000, 0, False), "wakes": (5, 320, 2, True), "query": (1, 64, 2, True)}
# What a measure changes of the global tables: its own default of a criterion or of a service setting.
JUDGING_DEFAULTS = {
    "commits": {},
    "cells": {"memory": {"max_pct": 90.0}, "latency": {"enabled": False}},
    "wakes": {"latency": {"percentile": "p95", "max_ms": 500.0}},
    "query": {"latency": {"max_ms": 1000.0}},
}
SERVICE_DEFAULTS = {"commits": {}, "cells": {"idle_timeout_ms": 86400000, "max_cells": 10000000}, "wakes": {"idle_timeout_ms": 5000}, "query": {}}
VARIANTS = {  # name -> default value of the variant table; None keys: any name
    "commits": {"small": {}, "large": {"steps": {"start": 5.0, "max": 320.0}}},
    "cells": {"default": {}, "cache256": {"service": {"cache_kib": 256}}},
    "wakes": {},
    "query": {},
}
OPEN_VARIANTS = ("cells",)


def variant_table(measure):
    return Table({
        "steps": steps_table(*RAMPS[measure][:2], sparse=True),
        "store": Table(STORE_LEAVES, sparse=True),
        "service": Table(SERVICE_LEAVES, sparse=True),
    }, sparse=True)


def measure_table(name):
    start, maximum, refine, confirm = RAMPS[name]
    names = tuple(VARIANTS[name])
    return Table({
        "steps": steps_table(start, maximum, refine, confirm),
        "store": Table(STORE_LEAVES, sparse=True),
        "service": Table(SERVICE_LEAVES, sparse=True, default=SERVICE_DEFAULTS[name]),
        "judging": Table(judging_children(True), sparse=True, default=JUDGING_DEFAULTS[name]),
        "workload": Table(WORKLOADS[name]),
        "variants": Leaf("list", list(names), item="str"),
        "variant": Map(None if name in OPEN_VARIANTS else names, lambda _, n=name: variant_table(n), "variant",
                       {k: v for k, v in VARIANTS[name].items()}),
    })


def scenario_table(cpus, memory):
    return Table({"cpus": positive(cpus), "memory": Leaf("size", memory), "data_tmpfs": Leaf("size", "1g")})


SCHEMA = Table({
    "round": Table({
        "cases": Leaf("list", [], item="str"),
        "scenarios": Leaf("list", ["A"], item="str", choices=SCENARIOS, nonempty=True),
        "measures": Leaf("list", list(MEASURES), item="str", choices=MEASURES, nonempty=True),
        "step_timeout_s": positive(1800),
        "timeout_s": Leaf("float", 0.0, lo=0),
    }),
    "output": Table({"dir": Leaf("str", ""), "name": Leaf("str", "")}),
    "host": Table({"ssh": Leaf("str", ""), "repo": Leaf("str", ""), "results": Leaf("str", ""), "timeout_s": positive(172800),
                   "cgroup_root": Leaf("str", "/sys/fs/cgroup/system.slice")}),
    "scenarios": Map(SCENARIOS, lambda key: scenario_table(1, "512m") if key == "A" else scenario_table(2, "2g"), "scenario",
                     {"A": None, "B": None}),
    "store": Table(STORE_LEAVES),
    "service": Table(SERVICE_LEAVES),
    "generator": Table({"arrivals": Leaf("str", "poisson", choices=("poisson", "fixed")), "max_outstanding": Leaf("int", 5000, lo=1),
                        "drain_s": Leaf("float", 20.0, lo=0), "timeout_s": positive(15)}),
    "report": Table({"fit_min_r2": Leaf("float", 0.8, lo=0, hi=1), "fit_min_span": Leaf("float", 2.0, lo=1)}),
    "measures": Map(MEASURES, measure_table, "measure", {m: None for m in MEASURES}),
    "judging": Table({"attempts": Leaf("int", 5, lo=1), **judging_children(False)}),
})


# ---- reading and merging -------------------------------------------------------------------------------

def default_of(node):
    if isinstance(node, Leaf):
        return copy.deepcopy(node.default)
    if isinstance(node, Table):
        return copy.deepcopy(node.default) if node.sparse else {k: default_of(c) for k, c in node.children.items()}
    return {k: (default_of(node.child(k)) if v is None else v) for k, v in node.defaults.items()}


def deep_update(base, new):
    out = copy.deepcopy(base)
    for key, value in new.items():
        out[key] = deep_update(out[key], value) if isinstance(value, dict) and isinstance(out.get(key), dict) else copy.deepcopy(value)
    return out


def unknown(path, key, known):
    close = difflib.get_close_matches(key, list(known), n=1)
    return f"{path}: unknown key" + (f" (did you mean {close[0]!r}?)" if close else f", expected one of {', '.join(known)}")


def join(path, key):
    return f"{path}.{key}" if path else key


def merge(node, value, path, problems):
    """`value` (from the file) checked against `node` and merged over its defaults; an invalid value is reported and the
    default stays."""
    if isinstance(node, Leaf):
        checked = node.check(path, value, problems)
        return node.default if checked is None else checked
    if not isinstance(value, dict):
        problems.append(f"{path}: expected a table, got {describe(value)}")
        return default_of(node)
    out = default_of(node)
    if isinstance(node, Table):
        for key, given in value.items():
            if key not in node.children:
                problems.append(unknown(join(path, key), key, node.children))
                continue
            before = len(problems)
            merged = merge(node.children[key], given, join(path, key), problems)
            if node.sparse and len(problems) > before:  # a sparse table keeps only what is given and valid
                continue
            out[key] = deep_update(out.get(key, {}), merged) if node.sparse and isinstance(merged, dict) else merged
        return out
    for key, given in value.items():
        if node.keys is not None and key not in node.keys:
            problems.append(f"{join(path, key)}: unknown {node.label} {key!r}, expected one of {', '.join(node.keys)}")
        elif node.keys is None and not NAME.match(key):
            problems.append(f"{join(path, key)}: a {node.label} name is lowercase letters, digits and underscores, starting with a letter")
        else:
            merged = merge(node.child(key), given, join(path, key), problems)
            out[key] = deep_update(out[key], merged) if isinstance(out.get(key), dict) and isinstance(merged, dict) else merged
    return out


# ---- the environment overrides ---------------------------------------------------------------------------

# env name (after ALAMBIQUE_LOAD_) -> (kind, key). A key is a path in the config; `steps`, `store` and `service` keys apply to
# the global table, to every measure and to every variant (an override of the file wins over the default of a measure too).
ENV_KEYS = {
    "STEP_S": ("steps", "duration_s"), "REFINE": ("steps", "refine"), "WAKES_STEP_S": ("path", "measures.wakes.steps.duration_s"),
    "ARRIVALS": ("path", "generator.arrivals"), "MAX_OUTSTANDING": ("path", "generator.max_outstanding"),
    "DRAIN_S": ("path", "generator.drain_s"), "OP_TIMEOUT_S": ("path", "generator.timeout_s"),
    "FIT_MIN_R2": ("path", "report.fit_min_r2"), "FIT_MIN_SPAN": ("path", "report.fit_min_span"),
    "ATTEMPTS": ("path", "judging.attempts"),
    "DELIVERY_MIN": ("judging", "delivery.min_ratio"), "MAX_ERROR_PCT": ("judging", "errors.max_pct"),
    "MAX_BUSY_PCT": ("judging", "busy.max_pct"), "COOLDOWN_MEM_PCT": ("judging", "cooldown.max_mem_pct"),
    "COOLDOWN_MAX_S": ("judging", "cooldown.max_wait_s"), "SUSPECT_THROTTLE_PCT": ("judging", "suspect_throttle.max_pct"),
    "MAX_NET_LOAD": ("judging", "host_load.max"), "SETTLE_S": ("judging", "host_load.quiet_window_s"),
    "WAIT_QUIET_STEP_S": ("judging", "host_load.quiet_max_wait_s"), "MAX_DRIVER_SWAP_MB": ("judging", "driver_swap.max_mb"),
    "MAX_STORE_SWAP_MB": ("judging", "store_swap.max_growth_mb"), "MAX_HOST_SWAP_MB": ("judging", "host_swap.max_mb"),
    "MIN_AVAILABLE_GIB": ("judging", "host_memory.min_available_gib"), "START_LOAD": ("judging", "host_start.max_load"),
    "WAIT_QUIET_S": ("judging", "host_start.max_wait_s"), "STORE_FULL_PCT": ("judging", "store_full.max_pct"),
    "STORE_ERROR_PCT": ("judging", "store_full.error_pct"), "STORE_MIN_FREE_MB": ("judging", "store_full.min_free_mb"),
    "DATA_ESTIMATE_PCT": ("judging", "data_full.estimate_pct"), "DATA_FULL_PCT": ("judging", "data_full.full_pct"),
    "CELLS_MEM_PCT": ("path", "measures.cells.judging.memory.max_pct"),
    "COMMITS_P99_MS": ("path", "measures.commits.judging.latency.max_ms"),
    "WAKES_P95_MS": ("path", "measures.wakes.judging.latency.max_ms"),
    "QUERY_P99_MS": ("path", "measures.query.judging.latency.max_ms"),
    "STORE_LATENCY_MS": ("store", "latency_ms"), "STORE_ON": ("store", "backing"), "STORE_DIR": ("store", "dir"),
    "STORE_IMAGE": ("store", "image"),
    "IMAGE": ("service", "image"), "ERL_AFLAGS": ("service", "erl_aflags"),
    "WAL_CHECKPOINT_BYTES": ("service", "wal_checkpoint_bytes"), "IDLE_TIMEOUT_MS": ("service", "idle_timeout_ms"),
    "MAX_CELLS": ("service", "max_cells"), "CACHE_KIB": ("service", "cache_kib"), "OPTS": ("service", "opts"),
    "COMMIT_CELLS": ("path", "measures.commits.workload.cells"), "COMMIT_ROWS": ("path", "measures.commits.workload.rows"),
    "CELLS_FILL_RATE": ("path", "measures.cells.workload.fill_rate"), "HOLD_S": ("path", "measures.cells.workload.hold_s"),
    "WAKE_MODE": ("path", "measures.wakes.workload.mode"), "TAIL_S": ("path", "measures.wakes.workload.tail_s"),
    "CELL_BYTES": ("path", "measures.wakes.workload.cell_bytes"),
    "QUERY_POPULATION": ("path", "measures.query.workload.population"),
    "QUERY_CELLS": ("path", "measures.query.workload.cells_per_query"),
    "CELL_ROWS": ("data", "cell_rows"), "ROW_BYTES": ("data", "row_bytes"), "POPULATE_CONCURRENCY": ("data", "populate_concurrency"),
    "CGROUP_ROOT": ("path", "host.cgroup_root"),
    "DATA_MB": ("scenario", "data_tmpfs"), "STORE_MB": ("store", "tmpfs_size"),
    "LEVELS": ("levels", "steps.values"),
}
ENV_PREFIX = "ALAMBIQUE_LOAD_"
MB_NAMES = ("DATA_MB", "STORE_MB")  # sizes in MB, as a bare number


def places(kind, key, raw=None):
    """Every path of the config an override of `kind`/`key` is set at."""
    if kind == "path":
        return [key]
    if kind == "scenario":
        return [f"scenarios.{s}.{key}" for s in SCENARIOS]
    if kind == "data":
        return [f"measures.{m}.workload.{key}" for m in ("wakes", "query")]
    if kind == "judging":
        return [f"judging.{key}"] + [f"measures.{m}.judging.{key}" for m in MEASURES]
    out = [] if kind == "steps" else [f"{kind}.{key}"]
    for m in MEASURES:
        named = set(VARIANTS[m]) | set(((raw or {}).get("measures", {}).get(m, {}) or {}).get("variant", {}) or {})
        out.append(f"measures.{m}.{kind}.{key}")
        out += [f"measures.{m}.variant.{v}.{kind}.{key}" for v in sorted(named)]
    return out


def put_path(raw, path, value):
    table = raw
    parts = path.split(".")
    for part in parts[:-1]:
        table = table.setdefault(part, {})
        if not isinstance(table, dict):
            return
    table[parts[-1]] = value


def parse_env(name, text, leaf, problems):
    """The value of an override variable parsed as `leaf` expects, or None after a problem."""
    try:
        if leaf.kind == "int":
            value = int(text)
        elif leaf.kind == "float":
            value = float(text)
        elif leaf.kind == "bool":
            value = {"true": True, "false": False}[text.lower()]
        else:
            value = text
    except (ValueError, KeyError):
        problems.append(f"{ENV_PREFIX}{name}: {text!r} is not {'a whole number' if leaf.kind == 'int' else 'a number' if leaf.kind == 'float' else 'true or false'}")
        return None
    return leaf.check(f"{ENV_PREFIX}{name}", value, problems)


def apply_env(raw, env, problems):
    """The overrides of ALAMBIQUE_LOAD_* in `env` set into the raw config, so the file's own validation applies after them.
    Returns the raw config and the list of the variables that were applied."""
    raw, applied = copy.deepcopy(raw), []
    for name, (kind, key) in ENV_KEYS.items():
        text = env.get(ENV_PREFIX + name)
        if text is None or text == "":
            continue
        applied.append(name)
        if kind == "levels":
            try:
                values = [int(float(t)) for t in re.split(r"[,\s]+", text.strip()) if t]
            except ValueError:
                problems.append(f"{ENV_PREFIX}{name}: {text!r} is not a list of numbers")
                continue
            leaf = Leaf("list", None, item="int", lo=0, open_lo=True, ascending=True, nonempty=True)
            if leaf.check(ENV_PREFIX + name, values, problems) is None:
                continue
            for path in places("steps", "values") + places("steps", "kind"):
                put_path(raw, path, values if path.endswith("values") else "ladder")
            continue
        sample = places(kind, key, raw)[0]
        if name in MB_NAMES:
            value = parse_env(name, text, Leaf("int", None, lo=1), problems)
            value = None if value is None else f"{value}m"
        else:
            value = parse_env(name, text, schema_leaf(sample), problems)
        if value is not None:
            for path in places(kind, key, raw):
                put_path(raw, path, value)
    return raw, applied


def schema_leaf(path):
    """The leaf a config path is checked by; a variant path is checked as its measure's."""
    parts = path.split(".")
    if parts[0] == "measures":
        parts = ["measures", parts[1]] + (parts[4:] if parts[2] == "variant" else parts[2:])
    node = SCHEMA
    for key in parts:
        if isinstance(node, Table):
            node = node.children[key]
        else:
            node = node.child(key)
    return node


# ---- cases ---------------------------------------------------------------------------------------------

def variants_of(config, measure):
    return list(config["measures"][measure]["variants"])


def cases(config):
    """The cases of the round, in order: (scenario, measure, variant or None)."""
    chosen = config["round"]["cases"]
    out = []
    if chosen:
        for entry in chosen:
            scenario, measure, *rest = entry.split("/")
            names = rest or variants_of(config, measure) or [None]
            out += [(scenario, measure, v) for v in names]
        return out
    for scenario in config["round"]["scenarios"]:
        for measure in config["round"]["measures"]:
            out += [(scenario, measure, v) for v in variants_of(config, measure) or [None]]
    return out


def tag(case):
    return "/".join(p for p in case if p)


def case_config(config, scenario, measure, variant):
    """The settings of one case: the global tables with the overrides of its measure and variant applied. Without
    `host`, `levels` and the run's own fields this is what a step result depends on, and what is hashed."""
    entry = config["measures"][measure]
    vtable = entry["variant"].get(variant, {}) if variant else {}
    layer = lambda name, base: deep_update(deep_update(base, entry[name]), vtable.get(name, {}))
    steps = deep_update(entry["steps"], vtable.get("steps", {}))
    judging = deep_update({k: v for k, v in config["judging"].items() if k != "attempts"}, entry["judging"])
    scen = config["scenarios"][scenario]
    store = layer("store", config["store"])
    return {
        "scenario": scenario, "measure": measure, "variant": variant or "default",
        "cpus": scen["cpus"], "memory_mb": size_mb(scen["memory"]), "data_mb": size_mb(scen["data_tmpfs"]),
        "store": {**store, "tmpfs_mb": size_mb(store["tmpfs_size"])},
        "service": layer("service", config["service"]),
        "generator": config["generator"], "report": config["report"],
        "attempts": config["judging"]["attempts"],
        "steps": steps, "judging": judging, "workload": entry["workload"],
    }


def levels(steps):
    """The levels of the ramp: the ladder, or from `start` growing by `factor` (each level above the last) up to `max`."""
    if steps["kind"] == "ladder":
        return list(steps["values"])
    out, value = [], steps["start"]
    while value <= steps["max"]:
        level = max(math.floor(value + 0.5), out[-1] + 1 if out else 1)
        if level > steps["max"]:
            break
        out.append(level)
        value = max(value * steps["factor"], level * steps["factor"])
    return out


def config_hash(case):
    """A stable hash of the settings of a case: the same settings give the same hash, whatever the order or spelling in the file."""
    core = {k: v for k, v in case.items() if k not in SKIP_HASH}
    return hashlib.sha256(json.dumps(core, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


# ---- validation ----------------------------------------------------------------------------------------

def check_cases(config, problems):
    known = set()
    for i, entry in enumerate(config["round"]["cases"]):
        where = f"round.cases[{i}]"
        parts = entry.split("/")
        if not 2 <= len(parts) <= 3:
            problems.append(f"{where}: {entry!r} is not scenario/measure or scenario/measure/variant")
            continue
        scenario, measure, *rest = parts
        if scenario not in SCENARIOS:
            problems.append(f"{where}: unknown scenario {scenario!r}, expected one of {', '.join(SCENARIOS)}")
        elif measure not in MEASURES:
            problems.append(f"{where}: unknown measure {measure!r}, expected one of {', '.join(MEASURES)}")
        elif rest and rest[0] not in config["measures"][measure]["variant"]:
            problems.append(f"{where}: unknown variant {rest[0]!r} of {measure}, expected one of {', '.join(config['measures'][measure]['variant']) or 'none (it has no variants)'}")
        elif entry in known:
            problems.append(f"{where}: {entry!r} is listed twice")
        else:
            known.add(entry)


def check_variants(config, problems):
    for measure in MEASURES:
        entry = config["measures"][measure]
        if VARIANTS[measure] and not entry["variants"]:
            problems.append(f"measures.{measure}.variants: must not be empty")
        for v in entry["variants"]:
            if v not in entry["variant"]:
                problems.append(f"measures.{measure}.variants: unknown variant {v!r}, expected one of {', '.join(entry['variant']) or 'none'}")


def check_plan(where, steps, problems):
    if steps["kind"] == "ladder" and not steps["values"]:
        problems.append(f"{where}.values: a ladder needs values")
    if steps["kind"] == "bisect":
        if steps["values"]:
            problems.append(f"{where}.values: only a ladder uses values; set kind = \"ladder\" or remove them")
        if steps["start"] > steps["max"]:
            problems.append(f"{where}: start {steps['start']:g} is above max {steps['max']:g}")


def check_config(config, problems):
    check_variants(config, problems)
    check_cases(config, problems)
    for measure in MEASURES:
        entry = config["measures"][measure]
        if any(p.startswith(f"measures.{measure}.") and ".steps" in p.split(":")[0] for p in problems):
            continue  # a value that was refused stays at its default: its plan is not judged twice
        before = len(problems)
        check_plan(f"measures.{measure}.steps", entry["steps"], problems)
        for variant, table in entry["variant"].items():
            if table.get("steps") and len(problems) == before:  # a bad base plan is reported once
                check_plan(f"measures.{measure}.variant.{variant}.steps", deep_update(entry["steps"], table["steps"]), problems)
    problems[:] = list(dict.fromkeys(problems))
    for measure in MEASURES:
        w = config["measures"][measure]["workload"]
        if measure == "query" and w["cells_per_query"] > w["population"]:
            problems.append("measures.query.workload.cells_per_query: more than workload.population")
    host, output = config["host"], config["output"]
    if host["ssh"] and not host["repo"]:
        problems.append("host.repo: required when host.ssh is set")
    if host["ssh"] and not output["name"]:
        problems.append("output.name: required when host.ssh is set, so that --fetch finds the results")
    for key, pattern in (("ssh", r"[A-Za-z0-9_.@:-]*"), ("repo", r"[A-Za-z0-9_./~-]*"), ("results", r"[A-Za-z0-9_./~-]*")):
        if not re.fullmatch(pattern, host[key]):
            problems.append(f"host.{key}: {host[key]!r} has characters a remote command cannot take safely")
    if not re.fullmatch(r"[A-Za-z0-9._-]*", output["name"]):
        problems.append("output.name: use letters, digits, dot, dash and underscore only")
    store = config["store"]
    if store["backing"] == "tmpfs" and store["dir"]:
        problems.append("store.dir: only a store on disk (store.backing = \"disk\") has a directory")


def resolve(raw, env=None):
    """The config of `raw` (a dict as read from TOML) merged over the defaults, with the overrides of `env` (os.environ when
    None); ConfigError lists every problem."""
    problems = []
    raw, _ = apply_env(raw, os.environ if env is None else env, problems)
    config = merge(SCHEMA, raw, "", problems)
    check_config(config, problems)
    if problems:
        raise ConfigError(problems)
    return config


def load_config(path=None, env=None):
    """The resolved config of the TOML file at `path` (the defaults when None)."""
    if path is None:
        return resolve({}, env)
    try:
        with open(path, "rb") as file:
            raw = tomllib.load(file)
    except OSError as error:
        raise ConfigError([f"cannot read the file: {error.strerror}"], path) from error
    except tomllib.TOMLDecodeError as error:
        raise ConfigError([f"not valid TOML: {error}"], path) from error
    try:
        return resolve(raw, env)
    except ConfigError as error:
        raise ConfigError(error.problems, path) from None


def overrides_in(env):
    return sorted(name for name in ENV_KEYS if env.get(ENV_PREFIX + name))


# ---- the plan, the results directory and the cases --------------------------------------------------------

def results_dir(config, stamp=None):
    out = config["output"]
    return out["dir"] or os.path.join("results", "load", out["name"] or stamp or datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ"))


def case_dir(root, case):
    return os.path.join(root, *[p for p in case if p])


def step_files(directory):
    """The step results of a case directory: path -> parsed JSON (an unreadable file reads as {})."""
    out = {}
    if os.path.isdir(directory):
        for name in sorted(os.listdir(directory)):
            if re.fullmatch(r"step-[0-9]+(-[0-9]+)?\.json", name):
                try:
                    with open(os.path.join(directory, name), encoding="utf-8") as file:
                        out[name] = json.load(file)
                except (OSError, ValueError):
                    out[name] = {}
    return out


def done_steps(directory, case_hash):
    """The step results of a case that count as done: not invalid, with this hash."""
    return {n: s for n, s in step_files(directory).items() if s.get("verdict") in ("good", "bad") and s.get("config_hash") == case_hash}


def refused(root, items, skip_done):
    """With skip-done: the problems for a results directory whose stored results were made with other settings."""
    problems = []
    if not skip_done:
        return problems
    for case, case_cfg in items:
        directory = case_dir(root, case)
        want = config_hash(case_cfg)
        for name, step in step_files(directory).items():
            if step.get("verdict") in ("good", "bad") and step.get("config_hash") != want:
                problems.append(f"{os.path.join(directory, name)}: made with other settings (hash {str(step.get('config_hash'))[:12]}, this "
                                f"config {want[:12]}); use --no-skip-done to run it again, or another output.name")
    return problems


def command_of(path, env, skip_done):
    parts = [f"{ENV_PREFIX}{n}={shlex.quote(env[ENV_PREFIX + n])}" for n in overrides_in(env)]
    parts += ["mise run load -- --config", shlex.quote(path)] + ([] if skip_done else ["--no-skip-done"])
    return " ".join(parts)


def prepare(config, path, env, skip_done, stamp=None, deadline=0):
    """Write config.json and a case.json per case into the results directory; returns the directory and the cases."""
    root = results_dir(config, stamp)
    items = [(c, case_config(config, *c)) for c in cases(config)]
    problems = refused(root, items, skip_done)
    if problems:
        raise ConfigError(problems, path)
    os.makedirs(root, exist_ok=True)
    out = []
    for case, case_cfg in items:
        directory = case_dir(root, case)
        os.makedirs(directory, exist_ok=True)
        steps = case_cfg["steps"]
        run = {**case_cfg, "hash": config_hash(case_cfg), "levels": levels(steps), "dir": directory, "skip_done": skip_done,
               "command": command_of(path, env, skip_done), "host": {"cgroup_root": config["host"]["cgroup_root"]},
               "timeouts": {"step_s": config["round"]["step_timeout_s"], "round_s": config["round"]["timeout_s"]}}
        write_json(os.path.join(directory, "case.json"), run)
        out.append((case, os.path.join(directory, "case.json")))
    write_json(os.path.join(root, "config.json"), {"config": config, "cases": [tag(c) for c, _ in out]})
    return root, out


def write_json(path, data):
    tmp = f"{path}.tmp-{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=1, sort_keys=True)
        file.write("\n")
    os.replace(tmp, path)


def dry_run(config, path, env, skip_done):
    """The plan of the round as text: every case with its levels, criteria and how many of its steps are done."""
    root = results_dir(config, "<time stamp>")
    items = cases(config)
    lines = [f"round {config['output']['name'] or '(unnamed)'}: {len(items)} cases, results in {root}",
             f"skip-done {'on' if skip_done else 'off'}" + (f", ran with {', '.join(ENV_PREFIX + n for n in overrides_in(env))}" if overrides_in(env) else "")]
    if config["host"]["ssh"]:
        lines.append(f"host {config['host']['ssh']} (--remote runs the round there, --fetch copies the results back)")
    lines.append(f"timeouts: {config['round']['step_timeout_s']:g} s a step, round {'none' if not config['round']['timeout_s'] else format(config['round']['timeout_s'], 'g') + ' s'}")
    total = 0.0
    for n, case in enumerate(items, 1):
        c = case_config(config, *case)
        steps, lv = c["steps"], levels(c["steps"])
        most = len(lv) + steps["refine"] + (1 if steps["confirm"] else 0)
        total += most * steps["duration_s"]
        done = len(done_steps(case_dir(root, case), config_hash(c))) if skip_done else 0
        store = c["store"]
        where = f"tmpfs {store['tmpfs_size']}" if store["backing"] == "tmpfs" else "disk"
        lines += [f"{n}. {tag(case)}: {c['cpus']:g} CPU, {c['memory_mb']} MB, data tmpfs {c['data_mb']} MB; store on {where}, +{store['latency_ms']:g} ms",
                  f"   steps {steps['kind']} {lv} then {steps['refine']} refinements{', 1 confirmation' if steps['confirm'] else ''}, {steps['duration_s']:g} s each, at most {most}"
                  + (f", {done} already done" if done else "")]
        judged = ", ".join(k for k, v in c["judging"].items() if v.get("enabled"))
        off = ", ".join(k for k, v in c["judging"].items() if not v.get("enabled"))
        lines.append(f"   judged: {judged}" + (f"; off: {off}" if off else ""))
    lines.append(f"at most {total / 3600:.1f} h of steps, without waits and preparation")
    return "\n".join(lines)


def shell_of(case):
    """The settings of a case that scripts/load-case needs, as shell assignments."""
    store, judging = case["store"], case["judging"]
    host_memory, start = judging["host_memory"], judging["host_start"]
    values = {
        "scenario": case["scenario"], "measure": case["measure"], "variant": case["variant"], "cpus": case["cpus"],
        "limit_mb": case["memory_mb"], "data_mb": case["data_mb"], "store_on": store["backing"], "store_mb": store["tmpfs_mb"],
        "store_dir": store["dir"], "store_image": store["image"], "image": case["service"]["image"],
        "erl_aflags": case["service"]["erl_aflags"], "out": case["dir"], "command": case["command"],
        "skip_done": "1" if case["skip_done"] else "0",
        "start_load": start["max_load"] if start["enabled"] else 1e9, "start_wait_s": start["max_wait_s"],
        "min_gib": host_memory["min_available_gib"] if host_memory["enabled"] else 0,
    }
    return "\n".join(f"{k}={shlex.quote(str(v))}" for k, v in values.items())


def remote_env(config):
    """Where a round runs with --remote and where its results are, as shell assignments (the paths are relative to the
    repository on the host unless absolute)."""
    host, name = config["host"], config["output"]["name"]
    values = {"ssh": host["ssh"], "repo": host["repo"], "results": host["results"] or os.path.join("results", "load", name),
              "timeout_s": f"{host['timeout_s']:.0f}", "local": results_dir(config)}
    return "\n".join(f"{k}={shlex.quote(str(v))}" for k, v in values.items())


def service_env(case):
    """The environment file of the service container: what the service reads (Alambique.Load.Service.App)."""
    svc = case["service"]
    pairs = {"STORE_LATENCY_MS": f"{case['store']['latency_ms']:g}", "WAL_CHECKPOINT_BYTES": svc["wal_checkpoint_bytes"]}
    for key, name in (("idle_timeout_ms", "IDLE_TIMEOUT_MS"), ("max_cells", "MAX_CELLS"), ("cache_kib", "CACHE_KIB")):
        if svc[key]:
            pairs[name] = svc[key]
    if svc["opts"]:
        pairs["OPTS"] = svc["opts"]
    return "\n".join(f"{ENV_PREFIX}{k}={v}" for k, v in pairs.items())


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--check", metavar="FILE", help="validate FILE and print the resolved config and its hash")
    group.add_argument("--dry-run", metavar="FILE", help="print what the round would run")
    group.add_argument("--prepare", metavar="FILE", help="validate FILE and write the results directory's config.json and case files")
    group.add_argument("--shell", metavar="CASE", help="print the settings of a case.json as shell assignments")
    group.add_argument("--service-env", metavar="CASE", help="print the service container's environment file of a case.json")
    group.add_argument("--remote-env", metavar="FILE", help="print where the round runs and where its results are, as shell assignments (needs host.ssh)")
    group.add_argument("--defaults", action="store_true", help="print the built-in defaults")
    parser.add_argument("--no-skip-done", action="store_true", help="run every step again (--prepare, --dry-run)")
    parser.add_argument("--output-dir", metavar="DIR", help="results directory, over output.dir (--prepare, --dry-run)")
    args = parser.parse_args()
    skip_done = not args.no_skip_done
    try:
        if args.shell or args.service_env:
            with open(args.shell or args.service_env, encoding="utf-8") as file:
                case = json.load(file)
            print(shell_of(case) if args.shell else service_env(case))
            return
        path = args.check or args.dry_run or args.prepare or args.remote_env
        config = load_config(path)
        if args.output_dir:
            config["output"]["dir"] = args.output_dir
        if args.remote_env:
            if not config["host"]["ssh"]:
                raise ConfigError(["host.ssh: not set, so there is no host to run on or fetch from"], path)
            print(remote_env(config))
        elif args.check:
            print(json.dumps(config, indent=1, sort_keys=True))
        elif args.dry_run:
            print(dry_run(config, path, os.environ, skip_done))
        elif args.prepare:
            root, out = prepare(config, path, os.environ, skip_done)
            print(f"{root}\t{config['round']['timeout_s']:g}")
            for (scenario, measure, variant), case_path in out:
                print(f"{scenario} {measure} {variant or 'default'} {case_path}")
        else:
            print(json.dumps(load_config(None, {}), indent=1, sort_keys=True))
    except ConfigError as error:
        print(error, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
