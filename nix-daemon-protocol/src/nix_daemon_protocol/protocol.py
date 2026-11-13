"""Generic protocol-level enums and types."""

from __future__ import annotations

from enum import IntEnum


class FileIngestionMethod(IntEnum):
    """Method for ingesting files into the store."""

    FLAT = 0
    NIX_ARCHIVE = 1


class GCAction(IntEnum):
    """Actions for collect-garbage operation."""

    RETURN_LIVE = 0
    RETURN_DEAD = 1
    DELETE_DEAD = 2
    DELETE_SPECIFIC = 3


class OptTrusted(IntEnum):
    """Trust status for a path or user."""

    UNKNOWN = 0
    TRUSTED = 1
    NOT_TRUSTED = 2


class Verbosity(IntEnum):
    """Log verbosity levels."""

    ERROR = 0
    WARN = 1
    NOTICE = 2
    INFO = 3
    TALKATIVE = 4
    CHATTY = 5
    DEBUG = 6
    VOMIT = 7


class ActivityType(IntEnum):
    """Tracked activity types in the daemon."""

    UNKNOWN = 0
    COPY_PATH = 100
    FILE_TRANSFER = 101
    REALISE = 102
    COPY_PATHS = 103
    BUILDS = 104
    BUILD = 105
    OPTIMISE_STORE = 106
    VERIFY_PATHS = 107
    SUBSTITUTE = 108
    QUERY_PATH_INFO = 109
    POST_BUILD_HOOK = 110
    BUILD_WAITING = 111
    FETCH_TREE = 112
    STRINGLY = 10113
    """A named activity told to a peer that reads no name.

    Determinate v3.24.0's `actStringly`, at `logging.hh:39`.
    """


class ResultType(IntEnum):
    """Result types for activities."""

    FILE_LINKED = 100
    BUILD_LOG_LINE = 101
    UNTRUSTED_PATH = 102
    CORRUPTED_PATH = 103
    SET_PHASE = 104
    PROGRESS = 105
    SET_EXPECTED = 106
    POST_BUILD_LOG_LINE = 107
    FETCH_STATUS = 108


class FieldType(IntEnum):
    """Types for activity/result fields."""

    INT = 0
    STRING = 1
