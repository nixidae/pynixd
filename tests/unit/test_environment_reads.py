"""The four environment variables that pynixd reads, without `environs`.

Issue #30 took `environs` out of the library. It answered four calls across
three modules and cost 64 ms of every daemon start, because it pulls
`marshmallow` and `python-dotenv`, and nothing ever called `env.read_env()`.
`pynixd/tests/_conftest/config.py` still uses it, which is what
nixidae's `CLAUDE.md` asks of a test.

`os.environ.get(name, "")` is exact for the two string reads.
`wire._env_int` is not exact, and this file pins the one place it differs:
`environs` refuses an empty value and this takes the default. That is the
better answer for a variable a shell may export empty, and it is a decision,
so it gets a test.

`signing.get_default_signing_key` keeps its own coverage in
`test_signing.py::TestGetDefaultSigningKey`.
"""

from __future__ import annotations

import pytest

from pynixd.psi import PsiWeights
from pynixd.wire import _env_int


class TestEnvInt:
    def test_an_absent_variable_gives_the_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("PYNIXD_TEST_INT", raising=False)

        assert _env_int("PYNIXD_TEST_INT", 17) == 17

    def test_a_value_wins_over_the_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PYNIXD_TEST_INT", "42")

        assert _env_int("PYNIXD_TEST_INT", 17) == 42

    def test_an_empty_value_gives_the_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """`PYNIXD_CHUNK_SIZE=` from a shell must not stop the daemon.

        `environs` raised on this. Taking the default is the deliberate
        difference, and the reason issue #30 could drop the library.
        """
        monkeypatch.setenv("PYNIXD_TEST_INT", "")

        assert _env_int("PYNIXD_TEST_INT", 17) == 17

    def test_a_value_that_is_not_a_number_still_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A wrong value is a fault to report, and not one to answer with a default."""
        monkeypatch.setenv("PYNIXD_TEST_INT", "half")

        with pytest.raises(ValueError, match="half"):
            _env_int("PYNIXD_TEST_INT", 17)


class TestPsiWeights:
    def test_no_variable_gives_the_declared_weights(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("PYNIXD_PSI_WEIGHTS", raising=False)

        assert PsiWeights.from_env() == PsiWeights()

    def test_an_empty_variable_gives_the_declared_weights(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PYNIXD_PSI_WEIGHTS", "")

        assert PsiWeights.from_env() == PsiWeights()

    def test_every_weight_comes_from_the_variable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PYNIXD_PSI_WEIGHTS", "cpu=0.5,mem=0.25,io=0.15,memfull=0.1")

        assert PsiWeights.from_env() == PsiWeights(cpu=0.5, mem=0.25, io=0.15, memfull=0.1)

    def test_a_subset_leaves_the_rest_declared(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PYNIXD_PSI_WEIGHTS", "cpu=0.9")

        weights = PsiWeights.from_env()

        assert weights.cpu == 0.9
        assert weights.mem == PsiWeights().mem
