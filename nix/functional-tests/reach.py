#!/usr/bin/env python3
"""State which tests of a pynixd run sent pynixd any operation.

A test passes against pynixd only in name when no operation of it reached
pynixd: the shim started pynixd, and the script then skipped, evaluated only,
or sent its commands past the daemon with `--store` or `NIX_REMOTE`. Nix's
suite does that in 27 scripts. `check_pynixd_served` counts the stores pynixd
opened, so it cannot tell such a test from one that pynixd answered.

Usage:

    reach.py PYNIXD.json TEST_DIR

PYNIXD.json is the `testlog.json` of the pynixd arm. TEST_DIR is
`$WORK/tmp/nix-test`, which holds `<suite>/<script>/pynixd.log` for each test:
the shim writes pynixd's log there. pynixd logs `client_op_timing` with
`total_ops` when a client session that sent an operation ends
(`pynixd/proxy.py`), and nothing for a session that sent none.

It needs the standard library alone, as `compare.py` does.
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

from compare import read_log


def operations(log: Path) -> int | None:
    """The operations pynixd answered in *log*, or None when the test has no log."""
    if not log.exists():
        return None
    total = 0
    with log.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if '"client_op_timing"' not in line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            ops = entry.get("total_ops")
            if isinstance(ops, int):
                total += ops
    return total


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    results, broken = read_log(argv[1])
    tests = Path(argv[2])

    reached: Counter[str] = Counter()
    unreached: dict[str, list[str]] = {}
    no_log: list[str] = []
    for name, result in sorted(results.items()):
        suite, _, script = name.partition(":")
        ops = operations(tests / suite / script / "pynixd.log")
        if ops is None:
            no_log.append(name)
        elif ops > 0:
            reached[result] += 1
        else:
            unreached.setdefault(result, []).append(name)

    print("=== REACH: tests that sent pynixd an operation ===")
    for result in sorted(set(reached) | set(unreached)):
        print(f"  {result:<6} {reached[result]:>4} reached, {len(unreached.get(result, [])):>4} not reached")
    if broken:
        print(f"  ({broken} unreadable line(s) of {argv[1]} dropped)")
    passes = unreached.get("OK", [])
    print(f"=== OK WITHOUT PYNIXD ({len(passes)}): a pass that tested nothing of pynixd ===")
    for name in passes:
        print(f"  {name}")
    if no_log:
        print(f"=== NO PYNIXD LOG ({len(no_log)}) ===")
        for name in no_log:
            print(f"  {name}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
