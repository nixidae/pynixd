"""A red test's log directory has a name that `actions/upload-artifact` accepts."""

from __future__ import annotations

from tests._conftest.constants import log_folder_name


def test_a_parametrized_id_loses_what_an_artifact_refuses() -> None:
    """The id of run 41204781849 that cost the run every log."""
    name = log_folder_name(
        "test_no_space_left_holds_the_store_back[error: writing to file: No space left on device-True]"
    )

    assert name == "test_no_space_left_holds_the_store_back_error__writing_to_file__No_space_left_on_device-True_"
    assert not set(name) & set('":<>|*?\r\n')
