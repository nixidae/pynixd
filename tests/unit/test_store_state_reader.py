"""The store reader of the functional-test harness reports a real difference.

`nix/functional-tests/store-state.py` is the store half of the stream mode:
it reads what each test left in its store, and the harness compares the two
runs. A reader that answers "the two agree" whatever it is given would pass
every run and measure nothing, and this repository has shipped that failure
twice already -- `ruff format .` and `ruff check --fix .` both rewrote the
tree and exited 0 while standing as gates.

So these tests are negative controls. Each one puts a known difference in
front of the comparison and states that the comparison finds it.

The module has a hyphen in its name, so it loads by path.
"""

from __future__ import annotations

import copy
import importlib.util
import json
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

import pytest

_SOURCE = Path(__file__).parent.parent.parent / "nix" / "functional-tests" / "store-state.py"


def _load():
    spec = importlib.util.spec_from_file_location("nixft_store_state", _SOURCE)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {_SOURCE}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


store_state = _load()

# The schema of Nix, copied from a store this suite built. `ValidPaths.hash` is
# base16 and `ca` is the content address, and the reader keeps both whole.
_SCHEMA = """
create table ValidPaths (
    id integer primary key autoincrement not null,
    path text unique not null,
    hash text not null,
    registrationTime integer not null,
    deriver text,
    narSize integer,
    ultimate integer,
    sigs text,
    ca text
);
create table Refs (
    referrer integer not null,
    reference integer not null,
    primary key (referrer, reference)
);
create table DerivationOutputs (
    drv integer not null,
    id text not null,
    path text not null,
    primary key (drv, id)
);
"""

_REALISATION_SCHEMA = """
create table Realisations (
    id integer primary key autoincrement not null,
    drvPath text not null,
    outputName text not null,
    outputPath integer not null,
    signatures text
);
create table RealisationsRefs (
    referrer integer not null,
    realisationReference integer
);
"""

# The tables pynixd adds to the same file. They are the work of the proxy and
# say nothing about the store, so the reader must not read them.
_PYNIXD_SCHEMA = """
create table PynixdSchema (id integer primary key check (id = 1), version integer not null, updatedAt integer not null);
create table PynixdPathAccess (path text primary key, lastReferencedAt integer not null);
"""

_STORE = "/tmp/t/ca/build/store"


def _make_store(root: Path, *, realisations: bool = True, pynixd_tables: bool = False) -> Path:
    """A test store at `<root>/<suite>/<test>`, with a database a reader can open."""
    db_path = root / "ca" / "build" / "var" / "nix" / "db" / "db.sqlite"
    db_path.parent.mkdir(parents=True)
    with closing(sqlite3.connect(db_path)) as db:
        db.execute("PRAGMA synchronous = OFF")
        db.executescript(_SCHEMA)
        if realisations:
            db.executescript(_REALISATION_SCHEMA)
        if pynixd_tables:
            db.executescript(_PYNIXD_SCHEMA)

        db.execute(
            "insert into ValidPaths (id, path, hash, registrationTime, deriver, narSize, ultimate, sigs, ca) "
            "values (1, ?, 'sha256:aaa', 111, null, 208, 1, 'key:sig', 'text:sha256:bbb')",
            (f"{_STORE}/aaa-builder.sh",),
        )
        db.execute(
            "insert into ValidPaths (id, path, hash, registrationTime, deriver, narSize, ultimate, sigs, ca) "
            "values (2, ?, 'sha256:ccc', 222, ?, 400, null, null, null)",
            (f"{_STORE}/ccc-out", f"{_STORE}/ddd-thing.drv"),
        )
        db.execute("insert into Refs (referrer, reference) values (2, 1)")
        db.execute("insert into DerivationOutputs (drv, id, path) values (1, 'out', ?)", (f"{_STORE}/ccc-out",))
        if realisations:
            # Two rows for one output, which a real store does hold. `ca/build` of
            # Nix 2.34 leaves two identical rows for one `sha256:...!out`.
            db.execute(
                "insert into Realisations (id, drvPath, outputName, outputPath) values (1, 'sha256:ddd', 'out', 2)"
            )
            db.execute(
                "insert into Realisations (id, drvPath, outputName, outputPath) values (2, 'sha256:ddd', 'out', 2)"
            )
            db.execute(
                "insert into Realisations (id, drvPath, outputName, outputPath) values (3, 'sha256:eee', 'out', 1)"
            )
            db.execute("insert into RealisationsRefs (referrer, realisationReference) values (3, 1)")
        db.commit()
    return db_path


@pytest.fixture
def snapshot(tmp_path: Path) -> dict:
    _make_store(tmp_path)
    out = tmp_path / "snapshot.json"
    assert store_state._snapshot(tmp_path, out) == 0
    return json.loads(out.read_text())


class TestReader:
    def test_the_key_is_the_suite_and_the_test(self, snapshot) -> None:
        """The recording shim keys on `$TEST_SUITE_NAME/$TEST_NAME` too.

        A difference in the store and a difference on the wire must name the
        same test, or the two halves of the stream mode cannot be read side by
        side.
        """
        assert list(snapshot) == ["ca/build"]

    def test_a_path_keeps_the_facts_that_state_what_it_is(self, snapshot) -> None:
        facts = snapshot["ca/build"]["paths"]["ccc-out"]
        assert facts == {
            "nar_hash": "sha256:ccc",
            "nar_size": 400,
            "deriver": "ddd-thing.drv",
            "ca": None,
            "ultimate": False,
            "references": ["aaa-builder.sh"],
        }

    def test_the_store_directory_is_not_in_a_path(self, snapshot) -> None:
        """Every path holds the work directory, and two runs may not share one."""
        assert all("/" not in name for name in snapshot["ca/build"]["paths"])

    def test_the_volatile_fields_are_absent(self, snapshot) -> None:
        """`registrationTime` is a wall clock, and `sigs` names the signing key.

        `tests/differential/snapshot.py` states the same two reasons.
        """
        facts = snapshot["ca/build"]["paths"]["aaa-builder.sh"]
        assert "registration_time" not in facts
        assert "sigs" not in facts

    def test_null_ultimate_reads_as_false(self, snapshot) -> None:
        """The column takes null for false, and two engines must not disagree
        because one wrote 0 and the other wrote nothing."""
        assert snapshot["ca/build"]["paths"]["aaa-builder.sh"]["ultimate"] is True
        assert snapshot["ca/build"]["paths"]["ccc-out"]["ultimate"] is False

    def test_a_repeated_realisation_stays_repeated(self, snapshot) -> None:
        """A store that registers one realisation twice differs from a store
        that registers it once. `ca/duplicate-realisation-in-closure` is a test
        of this suite."""
        realisations = snapshot["ca/build"]["realisations"]
        assert len(realisations["sha256:ddd!out"]) == 2
        assert len(realisations["sha256:eee!out"]) == 1

    def test_a_realisation_keeps_its_references(self, snapshot) -> None:
        assert snapshot["ca/build"]["realisations"]["sha256:eee!out"][0]["references"] == ["sha256:ddd!out"]

    def test_the_output_map_is_read(self, snapshot) -> None:
        assert snapshot["ca/build"]["derivation_outputs"] == {"aaa-builder.sh": {"out": "ccc-out"}}

    def test_a_store_without_ca_derivations_reads_as_empty(self, tmp_path) -> None:
        """`Realisations` exists only when `ca-derivations` is on, and a store
        without it is a store and not an error."""
        _make_store(tmp_path, realisations=False)
        out = tmp_path / "snapshot.json"
        assert store_state._snapshot(tmp_path, out) == 0
        assert json.loads(out.read_text())["ca/build"]["realisations"] == {}

    def test_the_tables_of_pynixd_are_not_read(self, tmp_path) -> None:
        """pynixd writes its own tables into the store database. They are the
        work of the proxy, so an arm that has them must not differ for that."""
        _make_store(tmp_path, pynixd_tables=True)
        plain = tmp_path / "plain"
        plain.mkdir()
        _make_store(plain)
        with_tables = tmp_path / "a.json"
        without = tmp_path / "b.json"
        store_state._snapshot(tmp_path, with_tables)
        store_state._snapshot(plain, without)
        assert json.loads(with_tables.read_text())["ca/build"] == json.loads(without.read_text())["ca/build"]


class TestComparison:
    """Each test states one difference, and that the comparison finds it.

    A reader that answers "the two agree" whatever it is given passes every
    run and measures nothing.
    """

    @pytest.fixture
    def store(self, snapshot) -> dict:
        return snapshot["ca/build"]

    def test_a_store_agrees_with_itself(self, store) -> None:
        assert store_state._compare_one(store, store) == []

    def test_a_field_that_moved_is_reported(self, store) -> None:
        other = copy.deepcopy(store)
        other["paths"]["ccc-out"]["nar_size"] = 999
        lines = store_state._compare_one(store, other)
        assert len(lines) == 1
        assert "nar_size" in lines[0]
        assert "400" in lines[0]
        assert "999" in lines[0]

    def test_a_path_that_one_side_lacks_is_reported(self, store) -> None:
        other = copy.deepcopy(store)
        del other["paths"]["ccc-out"]
        lines = store_state._compare_one(store, other)
        assert any("ccc-out" in line and "pynixd does not" in line for line in lines)

    def test_a_path_that_only_pynixd_has_is_reported(self, store) -> None:
        other = copy.deepcopy(store)
        other["paths"]["zzz-extra"] = dict(other["paths"]["ccc-out"])
        lines = store_state._compare_one(store, other)
        assert any("zzz-extra" in line and "the daemon does not" in line for line in lines)

    def test_an_output_map_that_disagrees_is_reported(self, store) -> None:
        """The difference the first CA run found: `QueryDerivationOutputMap`."""
        other = copy.deepcopy(store)
        other["derivation_outputs"]["aaa-builder.sh"]["out"] = "zzz-wrong"
        lines = store_state._compare_one(store, other)
        assert any("derivation_outputs" in line and "zzz-wrong" in line for line in lines)

    def test_a_realisation_registered_once_instead_of_twice_is_reported(self, store) -> None:
        other = copy.deepcopy(store)
        other["realisations"]["sha256:ddd!out"] = other["realisations"]["sha256:ddd!out"][:1]
        lines = store_state._compare_one(store, other)
        assert any("realisations" in line and "sha256:ddd!out" in line for line in lines)

    def test_a_store_that_could_not_be_read_is_not_agreement(self, store) -> None:
        """An unreadable store holds no table, so every comparison finds
        nothing. "The two agree" is the one answer it must not give."""
        broken = {"unreadable": "database disk image is malformed"}
        assert store_state._compare_one(store, broken) != []
        assert store_state._compare_one(broken, store) != []

    def test_a_reference_that_went_missing_is_reported(self, store) -> None:
        """The failure a `BuildResult` hides: the build succeeded, and the path
        it registered points at nothing."""
        other = copy.deepcopy(store)
        other["paths"]["ccc-out"]["references"] = []
        lines = store_state._compare_one(store, other)
        assert any("references" in line for line in lines)


class TestNoise:
    """A test that disagrees with itself measures nothing, either way.

    `NOISE` names five, each measured by running one arm twice. Such a test
    cannot be called "different", and it cannot be called "same" either: when
    the two arms agree, one draw agreed with another.
    """

    @pytest.fixture
    def pair(self, tmp_path: Path, snapshot: dict) -> tuple[Path, Path]:
        """Two snapshots that disagree, under a name `NOISE` covers."""
        noisy = store_state.NOISE[0].test
        control = {noisy: snapshot["ca/build"]}
        candidate = copy.deepcopy(control)
        candidate[noisy]["paths"]["ccc-out"]["nar_size"] = 999

        left = tmp_path / "control.json"
        right = tmp_path / "candidate.json"
        left.write_text(json.dumps(control))
        right.write_text(json.dumps(candidate))
        return left, right

    def test_a_noisy_test_is_not_counted_as_a_difference(self, pair, capsys) -> None:
        left, right = pair
        code = store_state._compare(left, right)
        out = capsys.readouterr().out

        assert "NOISE     " in out
        assert "DIFFERENT " not in out
        assert "different: 0" in out
        assert "noise:     1" in out
        # Not a failure either: the run says nothing about this test.
        assert code == 0

    def test_a_noisy_test_still_reaches_the_report(self, pair) -> None:
        """Counted out of the answer, and not hidden from the reader.

        A difference that nobody can see is how a real one goes unnoticed once
        somebody widens this list.
        """
        left, right = pair
        store_state._compare(left, right)

        report = (right.parent / "store-report.txt").read_text()
        assert store_state.NOISE[0].test in report
        assert store_state.NOISE[0].reason in report
        assert "nar_size" in report

    def test_a_test_outside_the_list_is_still_a_difference(self, tmp_path, snapshot) -> None:
        """The list names five tests, and covers no other."""
        control = {"ca/not-noisy": snapshot["ca/build"]}
        candidate = copy.deepcopy(control)
        candidate["ca/not-noisy"]["paths"]["ccc-out"]["nar_size"] = 999

        left = tmp_path / "a.json"
        right = tmp_path / "b.json"
        left.write_text(json.dumps(control))
        right.write_text(json.dumps(candidate))

        assert store_state._compare(left, right) == 1


class TestMissingAndExtra:
    """A store one arm left and the other did not.

    The two directions are different findings. A missing store is a test that
    ran against the daemon and left nothing under pynixd. An extra store is
    the other way, and over the full suite every one of the eight held zero
    paths: pynixd's startup handshake makes Nix construct a store that no
    client asked for. Issue #42.

    They shared one counter, and the summary then read "missing: 8" for a run
    that missed nothing.
    """

    def _write(self, tmp_path: Path, control: dict, candidate: dict) -> tuple[Path, Path]:
        left = tmp_path / "control.json"
        right = tmp_path / "candidate.json"
        left.write_text(json.dumps(control))
        right.write_text(json.dumps(candidate))
        return left, right

    def test_a_store_only_the_daemon_left_is_missing(self, tmp_path, snapshot, capsys) -> None:
        left, right = self._write(tmp_path, snapshot, {})
        code = store_state._compare(left, right)
        out = capsys.readouterr().out

        assert "MISSING   ca/build" in out
        assert "missing:   1" in out
        assert "extra:     0" in out
        assert code == 1

    def test_a_store_only_pynixd_left_is_extra(self, tmp_path, snapshot, capsys) -> None:
        left, right = self._write(tmp_path, {}, snapshot)
        code = store_state._compare(left, right)
        out = capsys.readouterr().out

        assert "EXTRA     ca/build" in out
        assert "extra:     1" in out
        assert "missing:   0" in out
        assert code == 1

    def test_an_extra_store_says_how_many_paths_it_holds(self, tmp_path, snapshot, capsys) -> None:
        """Zero paths is the shape of #42, and any other number is not.

        A line that named the test alone could not tell the two apart.
        """
        empty = {"main/nars": {"paths": {}, "derivation_outputs": {}, "realisations": {}}}
        left, right = self._write(tmp_path, {}, {**snapshot, **empty})
        store_state._compare(left, right)
        out = capsys.readouterr().out

        held = len(snapshot["ca/build"]["paths"])
        assert held > 0
        assert "EXTRA     main/nars (0 path(s))" in out
        assert f"EXTRA     ca/build ({held} path(s))" in out
