# nix-daemon-protocol

Declarative, transport-neutral Python codecs for Nix daemon protocol versions
1.35 through 1.38.

This subproject is independently buildable and releasable. It deliberately
contains only standard Nix daemon protocol types, serialization, and
deserialization diagnostics. pynixd-specific operations belong in `pynixd`.

Run the fast codec conformance suite with `pytest nix-daemon-protocol/tests`.
To profile representative serialization and deserialization, install the
`benchmark` extra and run `python nix-daemon-protocol/benchmarks/serde.py`.
