"""CollectGarbage operation — WireRequest/WireResponse types."""

from __future__ import annotations

from typing import ClassVar, Self

from pydantic import model_validator

from .constants import FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS
from .protocol import GCAction
from .store_path import StorePath
from .wire_message import WireField, WireModel
from .wire_ops import WireRequest, WireResponse


class GCPaths(WireModel):
    """`GCOptions::GCPaths` of Nix 2.35: the whole store, or named paths.

    `worker-protocol.cc:552`: a tag, then the paths and whether to delete
    their referrers when the tag is 0. The tag is 1 for the whole store.
    """

    whole_store: int = 0
    paths: set[StorePath] = WireField(default_factory=set, wire_depends_on=lambda self: self.whole_store == 0)
    delete_referrers: int = WireField(default=0, wire_depends_on=lambda self: self.whole_store == 0)


class CollectGarbageResponse(WireResponse):
    """CollectGarbage response — paths deleted + bytes freed."""

    paths_deleted: set[StorePath] = WireField(default_factory=set)
    bytes_freed: int
    obsolete: int


class CollectGarbageRequest(WireRequest):
    """CollectGarbage request — action, paths, and GC options."""

    op: ClassVar[int] = 20
    response_type = CollectGarbageResponse
    action: GCAction
    # **The paths are two fields, and a feature picks which one.**
    # `daemon.cc:745` of Nix 2.35 reads a `GCPaths` under
    # `delete-dead-specific-referrers`, and a bare set without it. pynixd
    # does not claim the feature, so it meets the second shape only in a
    # recording of two Nix peers.
    paths_to_delete: set[StorePath] = WireField(
        default_factory=set, unless_features=[FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS]
    )
    gc_paths: GCPaths | None = WireField(default=None, needs_features=[FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS])
    ignore_liveness: int
    max_freed: int
    obsolete1: int
    obsolete2: int
    obsolete3: int

    @model_validator(mode="after")
    def _fill_both_shapes(self) -> Self:
        """Fill the path field a constructor left out, so a writer of either shape has it.

        Decoding skips validation, so a decoded request takes `with_both_shapes`.
        """
        both = self.with_both_shapes()
        self.gc_paths = both.gc_paths
        self.paths_to_delete = both.paths_to_delete
        return self

    def with_both_shapes(self) -> CollectGarbageRequest:
        """This request with both path fields filled, so each peer reads its own shape.

        A writer keeps the field that its connection negotiated. A bare set
        becomes the `GCPaths` that `daemon.cc:748` of Nix 2.35 reads it as:
        the whole store for an empty set and any action but `DELETE_SPECIFIC`.
        """
        if self.gc_paths is None:
            whole = self.action != GCAction.DELETE_SPECIFIC and not self.paths_to_delete
            gc_paths = GCPaths(whole_store=1) if whole else GCPaths(paths=set(self.paths_to_delete))
            return self.model_copy(update={"gc_paths": gc_paths})
        paths = set() if self.gc_paths.whole_store else set(self.gc_paths.paths)
        return self.model_copy(update={"paths_to_delete": paths})

    def needs_the_feature(self) -> bool:
        """Whether only a peer with `delete-dead-specific-referrers` may run this.

        `daemon.cc:763` of Nix 2.35 refuses named paths for `DELETE_DEAD`
        without the feature, and the bare set has no room for the referrers.
        """
        gc_paths = self.with_both_shapes().gc_paths
        if gc_paths is None or gc_paths.whole_store:
            return False
        return bool(gc_paths.delete_referrers) or self.action == GCAction.DELETE_DEAD
