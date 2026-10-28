"""GC chunks follow the whole reference graph, rather than weight order alone."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from pynixd.gc import Collector, _referrer_first
from tests.unit.test_gc_collector import FakeContext, FakeLocal


def test_referrers_precede_references_across_priority_levels() -> None:
    assert _referrer_first(["child", "other", "parent"], {"parent": {"child"}}) == [
        "other",
        "parent",
        "child",
    ]


def test_self_references_do_not_block_deletion() -> None:
    assert _referrer_first(["a", "b"], {"a": {"a", "outside"}}) == ["a", "b"]


def test_cycles_stay_without_blocking_independent_paths() -> None:
    assert _referrer_first(["a", "b", "c"], {"a": {"b"}, "b": {"a"}}) == ["c"]


async def test_205_child_first_paths_delete_in_three_requests() -> None:
    paths = [f"/nix/store/{i:032d}-path" for i in range(205)]
    references = {paths[i]: {paths[i - 1]} for i in range(1, len(paths))}

    class GraphLocal(FakeLocal):
        async def execute(self, request: Any, **kwargs: Any) -> Any:
            assert kwargs == {"mark": False}
            return SimpleNamespace(
                infos=[
                    SimpleNamespace(path=path, info=SimpleNamespace(references=references.get(str(path), set())))
                    for path in request.paths
                ]
            )

    local = GraphLocal(refuse_until_gone={paths[i - 1]: paths[i] for i in range(1, len(paths))})
    collector = Collector(FakeContext(local=local, cache=None))  # type: ignore[arg-type]  # test store
    ordered = await collector._order_batch(local, paths)  # type: ignore[arg-type]  # test store
    assert ordered == paths[::-1]
    response = await collector._delete(ordered, {}, [], None)
    assert {str(path) for path in response.store_paths} == set(paths)
    assert local.sizes == [100, 100, 5]


async def test_missing_graph_metadata_plans_no_deletion() -> None:
    local = FakeLocal()
    collector = Collector(FakeContext(local=local, cache=None))  # type: ignore[arg-type]  # test store
    assert await collector._order_batch(local, ["/nix/store/00000000000000000000000000000000-path"]) == []  # type: ignore[arg-type]  # test store
