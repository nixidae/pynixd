"""`builtin:unpack-channel` (`builtins/unpack-channel.cc`) in the sandbox helper: a channel tarball, unpacked.

The helper runs this inside the chroot as the sandbox user, as `runChild` runs a
builtin (`unix/build/derivation-builder.cc:994`). The tarball goes through the
libarchive that Nix links, with the calls of `unpackTarfile` (`tarfile.cc:202`),
so formats, filters and error words are libarchive's own. The calls go to a
handle of our own: libarchive-c's wrappers raise on every code below zero and
log `ARCHIVE_WARN` through `logging`, and Nix tells the codes apart.

The library loads at import: inside the chroot no library is left to load.
"""

from __future__ import annotations

import ctypes
import os
from ctypes import POINTER, c_char_p, c_int, c_size_t, c_uint, c_void_p
from typing import TYPE_CHECKING, Any

from libarchive import ffi
from libarchive.extract import EXTRACT_SECURE_NODOTDOT, EXTRACT_SECURE_SYMLINKS, EXTRACT_TIME

from ..hintfmt import WARNING_PREFIX, magenta, show_error
from .buildenv import LVL_WARN, write_message

if TYPE_CHECKING:
    from .spec import UnpackChannelSpec

ARCHIVE_EOF, ARCHIVE_OK, ARCHIVE_WARN = 1, 0, -20
AE_IFDIR = 0o040000
#: `extract_archive` (`tarfile.cc:157`).
EXTRACT_FLAGS = EXTRACT_TIME | EXTRACT_SECURE_SYMLINKS | EXTRACT_SECURE_NODOTDOT
#: `TarArchive::check`'s default reason (`tarfile.hh:17`).
EXTRACT_REASON = "failed to extract archive (%s)"

_lib = ctypes.CDLL(ffi.libarchive_path)


def _bind(name: str, argtypes: list[Any], restype: Any) -> Any:
    function = getattr(_lib, f"archive_{name}")
    function.argtypes = argtypes
    function.restype = restype
    return function


_read_new = _bind("read_new", [], c_void_p)
_read_support_filter_all = _bind("read_support_filter_all", [c_void_p], c_int)
_read_support_format_tar = _bind("read_support_format_tar", [c_void_p], c_int)
_read_support_format_zip = _bind("read_support_format_zip", [c_void_p], c_int)
_read_support_format_empty = _bind("read_support_format_empty", [c_void_p], c_int)
_read_set_option = _bind("read_set_option", [c_void_p, c_char_p, c_char_p, c_char_p], c_int)
_read_open_filename = _bind("read_open_filename", [c_void_p, c_char_p, c_size_t], c_int)
_read_next_header = _bind("read_next_header", [c_void_p, POINTER(c_void_p)], c_int)
_read_extract = _bind("read_extract", [c_void_p, c_void_p, c_int], c_int)
_read_close = _bind("read_close", [c_void_p], c_int)
_read_free = _bind("read_free", [c_void_p], c_int)
_error_string = _bind("error_string", [c_void_p], c_char_p)
_entry_pathname = _bind("entry_pathname", [c_void_p], c_char_p)
_entry_copy_pathname = _bind("entry_copy_pathname", [c_void_p, c_char_p], None)
_entry_filetype = _bind("entry_filetype", [c_void_p], c_uint)
_entry_mode = _bind("entry_mode", [c_void_p], c_uint)
_entry_set_mode = _bind("entry_set_mode", [c_void_p, c_uint], None)
_entry_hardlink = _bind("entry_hardlink", [c_void_p], c_char_p)
_entry_copy_hardlink = _bind("entry_copy_hardlink", [c_void_p, c_char_p], None)


class UnpackError(Exception):
    pass


def _text(raw: bytes | None) -> str:
    return "" if raw is None else os.fsdecode(raw)


def _warn(text: str) -> None:
    write_message(LVL_WARN, WARNING_PREFIX + text)


class _Archive:
    """`TarArchive` over a file (`tarfile.cc:133`)."""

    def __init__(self, path: str) -> None:
        self.archive = _read_new()
        _read_support_filter_all(self.archive)
        _read_support_format_tar(self.archive)
        _read_support_format_zip(self.archive)
        _read_support_format_empty(self.archive)
        _read_set_option(self.archive, None, b"mac-ext", None)
        self.check(_read_open_filename(self.archive, os.fsencode(path), 16384), "failed to open archive: %s")

    def error(self) -> str:
        return _text(_error_string(self.archive))

    def check(self, code: int, reason: str = EXTRACT_REASON) -> None:
        """`TarArchive::check` with fatal warnings (`tarfile.cc:42`)."""
        if code == ARCHIVE_EOF:
            raise UnpackError("reached end of archive")
        if code != ARCHIVE_OK:
            raise UnpackError(reason.replace("%s", magenta(self.error()), 1))

    def free(self) -> None:
        _read_free(self.archive)


def _join(directory: str, rel: str) -> str:
    """`destDir / relPath`, where `relative_path` drops the leading separators."""
    return f"{directory}/{rel.lstrip('/')}"


def unpack_tarfile(tar_file: str, dest_dir: str) -> None:
    """`unpackTarfile` and `extract_archive` (`tarfile.cc:155`)."""
    archive = _Archive(tar_file)
    try:
        os.makedirs(dest_dir, exist_ok=True)
        entry = c_void_p()
        while True:
            code = _read_next_header(archive.archive, ctypes.byref(entry))
            if code == ARCHIVE_EOF:
                break
            name = _entry_pathname(entry)
            if name is None:
                raise UnpackError(f"cannot get archive member name: {magenta(archive.error())}")
            if code == ARCHIVE_WARN:
                _warn(f"getting archive member '{magenta(_text(name))}': {magenta(archive.error())}")
            else:
                archive.check(code)
            _entry_copy_pathname(entry, os.fsencode(_join(dest_dir, _text(name))))
            # Nix: "sources can and do contain dirs with no rx bits".
            mode = _entry_mode(entry)
            if _entry_filetype(entry) == AE_IFDIR and mode & 0o500 != 0o500:
                _entry_set_mode(entry, mode | 0o500)
            hardlink = _entry_hardlink(entry)
            if hardlink is not None:
                _entry_copy_hardlink(entry, os.fsencode(_join(dest_dir, _text(hardlink))))
            archive.check(_read_extract(archive.archive, entry, EXTRACT_FLAGS))
        archive.check(_read_close(archive.archive), "Failed to close archive (%s)")
    finally:
        archive.free()


def unpack_channel(out: str, channel_name: str | None, src: str | None) -> None:
    """`builtinUnpackChannel` (`builtins/unpack-channel.cc:6`)."""
    if channel_name is None:
        raise UnpackError(f"attribute '{magenta('channelName')}' missing")
    if src is None:
        raise UnpackError(f"attribute '{magenta('src')}' missing")
    # `path{channelName}.filename() != channelName` holds exactly for a name with a separator.
    if "/" in channel_name:
        raise UnpackError(f"channelName is not allowed to contain filesystem separators, got {magenta(channel_name)}")
    os.makedirs(out, exist_ok=True)
    unpack_tarfile(src, out)
    with os.scandir(out) as it:
        names = [entry.name for entry in it]
    if not names:
        raise UnpackError(f"channel tarball '{magenta(src)}' is empty")
    if len(names) != 1:
        raise UnpackError(f"channel tarball '{magenta(src)}' contains more than one file")
    file_name, target = f"{out}/{names[0]}", f"{out}/{channel_name}"
    try:
        os.rename(file_name, target)
    except OSError as e:
        raise UnpackError(
            f"failed to rename {magenta(file_name)} to {magenta(target)}: {os.strerror(e.errno or 0)}"
        ) from e


def run(spec: UnpackChannelSpec) -> None:
    """After `enterChroot`. Exits the process."""
    try:
        unpack_channel(spec["out"], spec["channel_name"], spec["src"])
    except Exception as e:  # noqa: BLE001 -- `runChild` writes any `std::exception` as the build's error
        os.write(2, (show_error(str(e)) + "\n").encode(errors="replace"))
        os._exit(1)
    os._exit(0)
