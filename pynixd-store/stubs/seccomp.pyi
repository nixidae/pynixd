"""The part of libseccomp's bindings (`src/python/seccomp.pyx`) the sandbox helper uses."""

ALLOW: int
MASKED_EQ: int

def ERRNO(errno: int) -> int: ...

class Arch:
    NATIVE: int
    X86: int
    X86_64: int
    X32: int
    ARM: int
    AARCH64: int

class Attr:
    CTL_NNP: int

class Arg:
    def __init__(self, arg: int, op: int, datum_a: int, datum_b: int = 0) -> None: ...

class SyscallFilter:
    def __init__(self, defaction: int) -> None: ...
    def add_arch(self, arch: int) -> None: ...
    def add_rule(self, action: int, syscall: int | str, *args: Arg) -> None: ...
    def set_attr(self, attr: int, value: int) -> None: ...
    def load(self) -> None: ...
