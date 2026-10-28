"""`DrvOutput`, and the one `StorePath` re-exported under its old name.

`StorePath` used to be defined here, and `nix_daemon_protocol.store_path` had
a second, different one. The two were not `==`, did not hash alike, and a
dict keyed by one never answered the other (#3). This class moved to
`nix_daemon_protocol.store_path`, which is where it has to live: the protocol
package decodes a store path and must not import `pynixd`.

The name stays here because most of pynixd imports it from here.
"""

from __future__ import annotations

from nix_daemon_protocol.store_path import StorePath as StorePath


class DrvOutput:
    """A Nix derivation output identifier.

    Format: ``<hash-algo>:<base16-hash>!<outputName>``
    Example: ``sha256:ba0770319c4c4c5f849e8e0e4a8b9c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8!out``

    The hash is the derivation's ``hashDerivationModulo`` — NOT the .drv store path.

    Can be constructed from a string or from keyword components::

        DrvOutput("sha256:abc!out")
        DrvOutput(hash_algo="sha256", hash_value="abc", output_name="out")
    """

    _hash_algo: str
    _hash_value: str
    _output_name: str
    _path: str

    def __init__(
        self,
        value: str = "",
        *,
        hash_algo: str | None = None,
        hash_value: str | None = None,
        output_name: str | None = None,
        path: str = "",
    ) -> None:
        if isinstance(value, DrvOutput):
            self._hash_algo = value._hash_algo
            self._hash_value = value._hash_value
            self._output_name = value._output_name
            self._path = value._path
            return
        if hash_algo is not None:
            self._hash_algo = hash_algo
            self._hash_value = hash_value or ""
            self._output_name = output_name or ""
            self._path = path
        else:
            if value and "!" not in value:
                raise ValueError(
                    f"Invalid DrvOutput: {value!r} — expected format '<algo>:<hash>!<outputName>'",
                )
            if value:
                id_hash, self._output_name = value.split("!", 1)
                # algo can have colons (e.g. "r:sha256"), so split from right
                *algo_parts, self._hash_value = id_hash.split(":")
                self._hash_algo = ":".join(algo_parts)
            else:
                self._hash_algo = ""
                self._hash_value = ""
                self._output_name = ""
            self._path = ""

    @property
    def hash_algo(self) -> str:
        return self._hash_algo

    @property
    def hash_value(self) -> str:
        return self._hash_value

    @property
    def output_name(self) -> str:
        return self._output_name

    @property
    def name(self) -> str:
        """Alias for ``output_name`` — matches the ``OutputInfo`` field."""
        return self._output_name

    @property
    def path(self) -> str:
        """The output store path, or empty for CA derivations."""
        return self._path

    @property
    def id_hash(self) -> str:
        """The hash-algorithm-prefixed hash part (before '!')."""
        return f"{self._hash_algo}:{self._hash_value}"

    def __str__(self) -> str:
        if not self._hash_algo and not self._hash_value and not self._output_name:
            return ""
        return f"{self._hash_algo}:{self._hash_value}!{self._output_name}"

    def __repr__(self) -> str:
        return f"DrvOutput({str(self)!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DrvOutput):
            return NotImplemented
        return (
            self._hash_algo == other._hash_algo
            and self._hash_value == other._hash_value
            and self._output_name == other._output_name
        )

    def __hash__(self) -> int:
        return hash((self._hash_algo, self._hash_value, self._output_name))
