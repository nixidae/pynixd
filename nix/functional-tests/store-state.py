#!/usr/bin/env python3
"""Read what each test left in its store, and compare two runs of that.

The stream mode compares the bytes on the wire. This compares the result. A
daemon can answer every request with the same bytes and still register a path
with the wrong references, and a daemon can answer differently and leave an
identical store. The two measures are independent, and a defect shows in one
or the other.

    snapshot TEST_ROOT_DIR OUT.json   read every test store under a run
    compare CONTROL.json CANDIDATE.json

The reader is `sqlite3` against `$TEST_ROOT/var/nix/db/db.sqlite`, opened read
only. Three reasons for the database rather than a client:

1. The stores are dead when this runs. `run.sh` stops each daemon with the
   test, so a client would have to start 200 more daemons to ask them.
2. The database is under both engines. A defect in a reader that sits above
   them could hide a difference; this sits below.
3. pynixd adds its own tables to the same file, and this reads none of them.
   `PynixdSchema`, `PynixdDerivationStats` and `PynixdPathAccess` are the work
   of the proxy and say nothing about the store.

`tests/differential/snapshot.py` states why `registrationTime` and `sigs` are
dropped, and this drops them for the same two reasons: the first is a wall
clock, and the second names the key of the store that signed.
"""

from __future__ import annotations

import dataclasses
import json
import sqlite3
import sys
from pathlib import Path

# The four tables of Nix. `Realisations` and `RealisationsRefs` exist only when
# `ca-derivations` is on, so a store without them reads as empty and not as an
# error.
_OPTIONAL_TABLES = ("Realisations", "RealisationsRefs")


def _base(path: str | None) -> str | None:
    """The name of a store path, without the directory that holds it.

    Every path in a test store begins with the test root, and the test root
    holds the work directory. Two runs under different `NIXFT_WORK` would then
    disagree on every path for no reason that is about a daemon.
    """
    if path is None or path == "":
        return None
    return path.rsplit("/", 1)[-1]


def _table_exists(db: sqlite3.Connection, name: str) -> bool:
    row = db.execute("select 1 from sqlite_master where type='table' and name=?", (name,)).fetchone()
    return row is not None


def _read_store(db_path: Path) -> dict[str, object]:
    """Every fact one store holds about itself."""
    db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        names: dict[int, str] = {}
        paths: dict[str, dict[str, object]] = {}
        for row in db.execute("select id, path, hash, deriver, narSize, ultimate, ca from ValidPaths"):
            name = _base(row["path"])
            if name is None:
                continue
            names[row["id"]] = name
            paths[name] = {
                "nar_hash": row["hash"],
                "nar_size": row["narSize"],
                "deriver": _base(row["deriver"]),
                "ca": row["ca"],
                # `null` means false in this column, and the two engines must
                # not disagree because one wrote 0 and the other wrote nothing.
                "ultimate": bool(row["ultimate"]),
                "references": [],
            }

        for row in db.execute("select referrer, reference from Refs"):
            referrer = names.get(row["referrer"])
            reference = names.get(row["reference"])
            if referrer is None or reference is None:
                continue
            references = paths[referrer]["references"]
            if isinstance(references, list):
                references.append(reference)
        for facts in paths.values():
            references = facts["references"]
            if isinstance(references, list):
                # Sorted: the order of a query is not a fact about the path.
                references.sort()

        outputs: dict[str, dict[str, str | None]] = {}
        for row in db.execute("select drv, id, path from DerivationOutputs"):
            drv = names.get(row["drv"])
            if drv is None:
                continue
            outputs.setdefault(drv, {})[row["id"]] = _base(row["path"])

        # A list for each key, and not one realisation. `Realisations` holds no
        # unique constraint over `(drvPath, outputName)`, and a store really
        # does hold two rows for one output: `ca/build` of Nix 2.34 leaves two
        # identical rows for `sha256:8f10...!out`. A store that registers the
        # same realisation twice differs from a store that registers it once,
        # and `ca/duplicate-realisation-in-closure` is a test of this suite.
        by_id: dict[int, dict[str, object]] = {}
        keys: dict[int, str] = {}
        if _table_exists(db, "Realisations"):
            for row in db.execute("select id, drvPath, outputName, outputPath from Realisations"):
                keys[row["id"]] = f"{row['drvPath']}!{row['outputName']}"
                by_id[row["id"]] = {"output_path": names.get(row["outputPath"]), "references": []}
        if _table_exists(db, "RealisationsRefs"):
            for row in db.execute("select referrer, realisationReference from RealisationsRefs"):
                entry = by_id.get(row["referrer"])
                reference = keys.get(row["realisationReference"])
                if entry is None or reference is None:
                    continue
                references = entry["references"]
                if isinstance(references, list):
                    references.append(reference)

        realisations: dict[str, list[dict[str, object]]] = {}
        for identifier, facts in by_id.items():
            references = facts["references"]
            if isinstance(references, list):
                references.sort()
            realisations.setdefault(keys[identifier], []).append(facts)
        for group in realisations.values():
            # Sorted by content: the row ids of two runs never agree, so the
            # order the rows arrive in is not a fact about the store.
            group.sort(key=lambda facts: json.dumps(facts, sort_keys=True))
    finally:
        db.close()

    return {"paths": paths, "derivation_outputs": outputs, "realisations": realisations}


def _snapshot(root: Path, out: Path) -> int:
    """Read every test store under *root*, and write one file.

    *root* is `$WORK/tmp/nix-test`, and `vars.sh` puts each test root at
    `<suite>/<test>` under it. That is the same key the recording shim uses, so
    a difference here and a difference on the wire name the same test.
    """
    if not root.is_dir():
        print(f"store-state: no test root at {root}", file=sys.stderr)
        return 2

    stores: dict[str, object] = {}
    for db_path in sorted(root.glob("*/*/var/nix/db/db.sqlite")):
        # `<suite>/<test>/var/nix/db/db.sqlite`, so four directories up.
        key = str(db_path.relative_to(root).parents[3])
        try:
            stores[key] = _read_store(db_path)
        except sqlite3.DatabaseError as error:
            # A test that the watchdog killed can leave a database mid-write.
            # One unreadable store must not lose the other 200.
            print(f"store-state: {key} is unreadable: {error}", file=sys.stderr)
            stores[key] = {"unreadable": str(error)}

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(stores, indent=1, sort_keys=True))
    total = sum(len(store.get("paths", {})) for store in stores.values() if isinstance(store, dict))
    print(f"store-state: {len(stores)} store(s), {total} path(s) -> {out}")
    return 0


def _differences(control: dict[str, object], candidate: dict[str, object], kind: str) -> list[str]:
    """Name each key the two do not agree on, for one of the three tables."""
    lines: list[str] = []
    left = control.get(kind, {})
    right = candidate.get(kind, {})
    if not isinstance(left, dict) or not isinstance(right, dict):
        return lines
    for name in sorted(set(left) - set(right)):
        lines.append(f"  {kind}: the daemon has {name} and pynixd does not")
    for name in sorted(set(right) - set(left)):
        lines.append(f"  {kind}: pynixd has {name} and the daemon does not")
    for name in sorted(set(left) & set(right)):
        left_facts = left[name]
        right_facts = right[name]
        if left_facts == right_facts:
            continue
        if isinstance(left_facts, dict) and isinstance(right_facts, dict):
            # Field by field, so a report names the one thing that moved
            # rather than two whole records a reader has to compare by eye.
            lines.extend(
                f"  {kind}: {name} {field}\n"
                f"      daemon: {left_facts.get(field)!r}\n"
                f"      pynixd: {right_facts.get(field)!r}"
                for field in sorted(set(left_facts) | set(right_facts))
                if left_facts.get(field) != right_facts.get(field)
            )
        else:
            lines.append(f"  {kind}: {name}\n      daemon: {left_facts!r}\n      pynixd: {right_facts!r}")
    return lines


@dataclasses.dataclass(frozen=True)
class Noise:
    """A test that does not build the same thing twice, and the reason."""

    test: str
    reason: str


# **These tests disagree with themselves, so they cannot answer this
# question.** Two runs of one arm differ, and no engine is why. A test here
# cannot be called "different", and it cannot be called "same" either: when
# the two arms agree, one draw agreed with another.
#
# Measured, and not guessed: two runs of the same arm over the `ca` suite of
# Nix 2.34. The daemon disagreed with itself on the first two, and pynixd on
# the first three. `main/nix-shell` and `main/structured-attrs` came later,
# from `record-control` twice over those two tests, so the daemon alone.
#
# Four of these five hold an `-env` derivation, and one variable is why.
# `NIX_BUILD_TOP` is `.../var/nix/builds/nix-<pid>-<random>`, so it is a new
# string in every run. It reaches the output because a `-env` derivation is a
# dump of the build environment, and the store path does not move with it: the
# derivation is input-addressed, so the same derivation keeps the same name and
# answers a new NAR hash. The lengths differ too, which is the `nar_size` delta
# of a few bytes that comes with each one.
#
# This is not the exemption table. An exemption covers a difference somebody
# explained and decided to keep, under a `NIX-DEFECT (#23)` or
# `NIX-DEVIATION (#27)` verdict, and there is no such table here yet -- issue
# #39. This covers a test that measures nothing, which is a different thing
# and needs no verdict.
NOISE: tuple[Noise, ...] = (
    Noise(
        test="ca/duplicate-realisation-in-closure",
        reason="builds a `current-time` derivation, so each run makes another content address",
    ),
    Noise(
        test="ca/nix-shell",
        reason="builds a `fixed-env` and a `shellDrv-env-dev`, which hold `NIX_BUILD_TOP`",
    ),
    Noise(
        test="ca/build",
        reason="registers one realisation twice in one run and once in the next",
    ),
    Noise(
        test="main/nix-shell",
        reason="builds a `fixed-env` and a `shellDrv-env-dev`, which hold `NIX_BUILD_TOP`",
    ),
    Noise(
        test="main/structured-attrs",
        reason="builds a `structured2-env-dev` and a `shellDrv-env-dev`, which hold `NIX_BUILD_TOP`",
    ),
)

_NOISE_BY_TEST = {item.test: item for item in NOISE}


def _compare_one(control: dict[str, object], candidate: dict[str, object]) -> list[str]:
    """Everything two runs of one test do not agree on."""
    lines: list[str] = []
    # An unreadable store holds no table, so every comparison below finds
    # nothing and the store would read as "agrees". That is the one answer it
    # must not give.
    for side, store in (("the daemon", control), ("pynixd", candidate)):
        if "unreadable" in store:
            lines.append(f"  the store of {side} could not be read: {store['unreadable']}")
    for kind in ("paths", "derivation_outputs", "realisations"):
        lines.extend(_differences(control, candidate, kind))
    return lines


def _compare(control_path: Path, candidate_path: Path) -> int:
    control = json.loads(control_path.read_text())
    candidate = json.loads(candidate_path.read_text())

    same = 0
    different = 0
    missing = 0
    extra = 0
    noise = 0
    report: list[str] = []

    for key in sorted(control):
        if key not in candidate:
            print(f"MISSING   {key}")
            missing += 1
            continue
        lines = _compare_one(control[key], candidate[key])
        if key in _NOISE_BY_TEST:
            # Reported either way, so that a reader sees the test and knows it
            # was read. It counts as neither side of the question.
            noise += 1
            print(f"NOISE     {key}")
            if lines:
                report.append(f"=== {key} (noise: {_NOISE_BY_TEST[key].reason}) ===")
                report.extend(lines)
                report.append("")
            continue
        if lines:
            different += 1
            print(f"DIFFERENT {key}")
            report.append(f"=== {key} ===")
            report.extend(lines)
            report.append("")
        else:
            same += 1

    # An extra store is a different finding from a missing one, and counting
    # the two in one number said "missing: 8" for a run that missed nothing.
    # The path count belongs on the line: a store of 0 paths is a state
    # directory that pynixd created and Nix did not, which is issue #42, and a
    # store that holds paths is something else.
    for key in sorted(set(candidate) - set(control)):
        held = len(candidate[key].get("paths", {}))
        print(f"EXTRA     {key} ({held} path(s))")
        extra += 1

    out = candidate_path.parent / "store-report.txt"
    out.write_text("\n".join(report) + "\n")
    print("=== STORE SUMMARY ===")
    print(f"same:      {same}")
    print(f"different: {different}")
    print(f"missing:   {missing}  (a store the daemon left and pynixd did not)")
    print(f"extra:     {extra}  (a store pynixd left and the daemon did not)")
    print(f"noise:     {noise}  (tests that disagree with themselves; see NOISE in store-state.py)")
    print(f"the differences are at {out}")
    return 1 if different or missing or extra else 0


def main(argv: list[str]) -> int:
    if len(argv) == 4 and argv[1] == "snapshot":
        return _snapshot(Path(argv[2]), Path(argv[3]))
    if len(argv) == 4 and argv[1] == "compare":
        return _compare(Path(argv[2]), Path(argv[3]))
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
