"""`builtin:buildenv` (`builtins/buildenv.cc`) in the sandbox helper: the symlink tree of a user environment.

The helper runs this inside the chroot as the sandbox user, as `runChild` runs a
builtin (`unix/build/derivation-builder.cc:994`). Its warnings are `@nix` lines,
because `runChild` installs Nix's JSON logger first (`:997`).
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import stat
from typing import TYPE_CHECKING

from ..hintfmt import WARNING_PREFIX, magenta, show_error

if TYPE_CHECKING:
    from collections.abc import Callable

    from .spec import BuildenvSpec

#: The entries `createLinks` leaves out of a profile (`buildenv.cc:69`), matched on the whole source path.
SKIPPED = (
    "/propagated-build-inputs",
    "/nix-support",
    "/perllocal.pod",
    "/info/dir",
    "/log",
    "/manifest.nix",
    "/manifest.json",
)
LVL_WARN = 1
_INT = re.compile(r"[+-]?[0-9]+")
_UINT = re.compile(r"\+?[0-9]+")

Package = tuple[str, bool, int]
"""A path, whether it is active, its priority."""


class BuildenvError(Exception):
    pass


def path_fmt(path: str) -> str:
    """`PathFmt` (`fmt.hh:114`) as a `HintFmt` argument: quoted, then magenta."""
    return magenta(f'"{path}"')


def packages(derivations: str) -> list[Package] | None:
    """The `derivations` attribute as `builtinBuildenv` reads it (`buildenv.cc:189`).

    `None` where Nix's `stoi` or `stoul` throws, or where Nix reads past the last word.
    """
    words = [w for w in re.split(r"[ \t\n\r]", derivations) if w]
    out: list[Package] = []
    i = 0
    while i < len(words):
        if i + 3 > len(words) or not _INT.fullmatch(words[i + 1]) or not _UINT.fullmatch(words[i + 2]):
            return None
        active, priority, count = words[i] != "false", int(words[i + 1]), int(words[i + 2])
        if not -(2**31) <= priority < 2**31 or i + 3 + count > len(words):
            return None
        out.extend((path, active, priority) for path in words[i + 3 : i + 3 + count])
        i += 3 + count
    return out


def _key(path: str) -> tuple[bytes, ...]:
    """The order of `std::filesystem::path`: element by element, bytes compared as `memcmp` does."""
    parts = [part for part in os.fsencode(path).split(b"/") if part]
    return (*((b"/",) if path.startswith("/") else ()), *parts, *((b"",) if path.endswith("/") and parts else ()))


def _join(directory: str, name: str) -> str:
    return directory + name if directory.endswith("/") else f"{directory}/{name}"


def _warn(text: str) -> None:
    """`warn` through `JSONLogger::log`. Not `fetchurl.log_message`: the daemon imports this module, and that one loads curl."""
    event = {"action": "msg", "level": LVL_WARN, "msg": WARNING_PREFIX + text}
    line = "@nix " + json.dumps(event, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
    os.write(2, line.encode(errors="replace"))


def _system_error(e: OSError, what: str) -> BuildenvError:
    return BuildenvError(f"{what}: {os.strerror(e.errno or 0)}")


def _maybe(stat_call: Callable[[str], os.stat_result], path: str) -> os.stat_result | None:
    """`maybeStat` and `maybeLstat` (`file-system.cc:186`): no file is `None`."""
    try:
        return stat_call(path)
    except (FileNotFoundError, NotADirectoryError):
        return None
    except OSError as e:
        raise _system_error(e, f"getting status of {path_fmt(path)}") from e


def _lstat(path: str) -> os.stat_result:
    try:
        return os.lstat(path)
    except OSError as e:
        raise _system_error(e, f"getting status of {path_fmt(path)}") from e


def _symlink(target: str, link: str) -> None:
    try:
        os.symlink(target, link)
    except OSError as e:
        raise _system_error(e, f"creating symlink {path_fmt(link)} -> {path_fmt(target)}") from e


def _create_links(priorities: dict[str, int], src_dir: str, dst_dir: str, priority: int) -> None:
    """`createLinks` (`buildenv.cc:33`). Entries come in directory order, as `DirectoryIterator` gives them."""
    try:
        with os.scandir(src_dir) as it:
            names = [entry.name for entry in it]
    except NotADirectoryError:
        _warn(f"not including {path_fmt(src_dir)} in the user environment because it's not a directory")
        return
    except OSError as e:
        raise _system_error(e, f"cannot read directory {path_fmt(src_dir)}") from e

    for name in names:
        if name.startswith("."):
            continue
        src_file, dst_file = _join(src_dir, name), _join(dst_dir, name)
        src_st = _maybe(os.stat, src_file)
        if src_st is None:
            _warn(f"skipping dangling symlink '{magenta(dst_file)}'")
            continue
        if src_file.endswith(SKIPPED):
            continue
        if stat.S_ISDIR(src_st.st_mode):
            dst_st = _maybe(os.lstat, dst_file)
            if dst_st is not None and stat.S_ISDIR(dst_st.st_mode):
                _create_links(priorities, src_file, dst_file, priority)
                continue
            if dst_st is not None and stat.S_ISLNK(dst_st.st_mode):
                target = os.path.realpath(dst_file)
                if not stat.S_ISDIR(_lstat(target).st_mode):
                    raise BuildenvError(f"collision between {path_fmt(src_file)} and non-directory {path_fmt(target)}")
                # Nix ignores the result of this `unlink`; a failure shows as the `mkdir` after it.
                with contextlib.suppress(OSError):
                    os.unlink(dst_file)
                try:
                    os.mkdir(dst_file, 0o755)
                except OSError as e:
                    raise _system_error(e, f"creating directory '{magenta(dst_file)}'") from e
                _create_links(priorities, target, dst_file, priorities.setdefault(dst_file, 0))
                _create_links(priorities, src_file, dst_file, priority)
                continue
        else:
            dst_st = _maybe(os.lstat, dst_file)
            if dst_st is not None and stat.S_ISLNK(dst_st.st_mode):
                previous = priorities.setdefault(dst_file, 0)
                if previous == priority:
                    raise BuildenvError(
                        "Unable to build profile. There is a conflict for the following files:\n\n"
                        f"  {path_fmt(os.readlink(dst_file))}\n  {path_fmt(src_file)}"
                    )
                if previous < priority:
                    continue
                os.unlink(dst_file)
            elif dst_st is not None and stat.S_ISDIR(dst_st.st_mode):
                raise BuildenvError(
                    f"collision between non-directory '{magenta(src_file)}' and directory '{magenta(dst_file)}'"
                )
        _symlink(src_file, dst_file)
        priorities[dst_file] = priority


def build_profile(out: str, pkgs: list[Package]) -> None:
    """`buildProfile` (`buildenv.cc:124`) into the directory *out*."""
    priorities: dict[str, int] = {}
    done: set[tuple[bytes, ...]] = set()
    postponed: dict[tuple[bytes, ...], str] = {}

    def add(pkg_dir: str, priority: int) -> None:
        if _key(pkg_dir) in done:
            return
        done.add(_key(pkg_dir))
        _create_links(priorities, pkg_dir, out, priority)
        propagated = os.path.join(pkg_dir, "nix-support", "propagated-user-env-packages")
        try:
            with open(propagated, "rb") as file:
                text = os.fsdecode(file.read())
        except (FileNotFoundError, NotADirectoryError):
            return
        except OSError as e:
            raise _system_error(e, f"opening file {path_fmt(propagated)}") from e
        for path in re.split(r"[ \n]", text):
            if path and _key(path) not in done:
                postponed.setdefault(_key(path), path)

    for path, active, priority in sorted(pkgs, key=lambda p: (p[2], _key(p[0]))):
        if active:
            add(path, priority)
    counter = 1000
    while postponed:
        round_ = sorted(postponed.items())
        postponed.clear()
        for _, path in round_:
            add(path, counter)
            counter += 1


def run(spec: BuildenvSpec) -> None:
    """`builtinBuildenv` (`buildenv.cc:173`), after `enterChroot`. Exits the process."""
    try:
        out = spec["out"]
        os.makedirs(out, exist_ok=True)
        pkgs, manifest = spec["packages"], spec["manifest"]
        if pkgs is None:
            raise BuildenvError(f"attribute '{magenta('derivations')}' missing")
        build_profile(out, [(path, active, priority) for path, active, priority in pkgs])
        if manifest is None:
            raise BuildenvError(f"attribute '{magenta('manifest')}' missing")
        _symlink(manifest, out + "/manifest.nix")
    except Exception as e:  # noqa: BLE001 -- `runChild` writes any `std::exception` as the build's error
        os.write(2, (show_error(str(e)) + "\n").encode(errors="replace"))
        os._exit(1)
    os._exit(0)
