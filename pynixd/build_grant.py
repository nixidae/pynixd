"""What a build pod may do on central's SSH listener: read the store, and add its own outputs. nixidae/nixidae#1560, spike.

Central makes a key for each pod it creates and grants that key one
derivation. The pod is a client only: it reads its inputs and adds the
derivation's outputs, unsigned. Any other operation closes the session, and
an add of any other path fails before a byte of it reaches the store.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final

from nix_daemon_protocol.add_multiple_to_store import AddMultipleToStoreRequest
from nix_daemon_protocol.add_temp_root import AddTempRootRequest
from nix_daemon_protocol.add_temp_roots import AddTempRootsRequest
from nix_daemon_protocol.add_to_store_nar import AddToStoreNarRequest
from nix_daemon_protocol.is_valid_path import IsValidPathRequest
from nix_daemon_protocol.nar_from_path import NarFromPathRequest
from nix_daemon_protocol.query_path_info import QueryPathInfoRequest
from nix_daemon_protocol.query_path_infos import QueryPathInfosRequest
from nix_daemon_protocol.query_valid_paths import QueryValidPathsRequest
from nix_daemon_protocol.set_options import SetOptionsRequest
from nix_daemon_protocol.store_dir import store_dir
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from pynixd_store.content_address import is_content_addressed

from .daemon_extensions.query_closure import QueryClosureRequest
from .daemon_extensions.query_closure_with_info import QueryClosureWithInfoRequest
from .exceptions import ClosingError

ADDS: Final = frozenset({AddToStoreNarRequest.op, AddMultipleToStoreRequest.op})
GRANTED_OPS: Final = ADDS | {
    request.op
    for request in (
        SetOptionsRequest,
        IsValidPathRequest,
        QueryValidPathsRequest,
        QueryPathInfoRequest,
        QueryPathInfosRequest,
        NarFromPathRequest,
        AddTempRootRequest,
        AddTempRootsRequest,
        QueryClosureRequest,
        QueryClosureWithInfoRequest,
    )
}
"""What a pod's copy in and copy out send, and nothing that builds, deletes or roots for good."""


@dataclass(frozen=True, slots=True)
class BuildGrant:
    drv_path: str
    outputs: frozenset[str]
    """The store paths the pod may add: the derivation's outputs, known before it builds."""

    def check_op(self, op_num: int) -> None:
        if op_num not in GRANTED_OPS:
            # Before `startWork`: the pod's session ends, as Nix ends one for an op it refuses.
            raise ClosingError(f"a build pod for {self.drv_path} may not run operation {op_num}")

    def check_add(self, info: ValidPathInfo) -> None:
        """An output, or a path its content address names: what a recursive build added, which an output refers to.

        A content-addressed path vouches for itself, as for any untrusted client (`pathInfoIsUntrusted`,
        `local-store.cc:1048`). The check is here because the session skips signatures for the whole add: the
        store then checks the files against the address, and this checks the address against the name.
        """
        path = str(info.path)
        if path in self.outputs:
            return
        try:
            named = is_content_addressed(path, str(info.info.ca or ""), map(str, info.info.references), store_dir())
        except ValueError:
            named = False
        if not named:
            raise ClosingError(
                f"a build pod for {self.drv_path} may not add {path}: it is not an output, and not content-addressed"
            )


@dataclass(slots=True)
class Grants:
    """The keys central gave its build pods, by their OpenSSH public key line."""

    by_key: dict[str, BuildGrant] = field(default_factory=dict)

    def grant(self, public_key: str, grant: BuildGrant) -> None:
        self.by_key[_key_text(public_key)] = grant

    def revoke(self, public_key: str) -> None:
        self.by_key.pop(_key_text(public_key), None)

    def of(self, public_key: str) -> BuildGrant | None:
        return self.by_key.get(_key_text(public_key))


def _key_text(public_key: str) -> str:
    """The key's type and base64, without the comment."""
    return " ".join(public_key.split()[:2])
