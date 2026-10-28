"""`/metrics` answers, and it is routed only when it is asked for.

pynixd has served Prometheus metrics since `metrics.py` was written, and no
deployment could reach them: the HTTP server starts only when `http_port` is
set, and that defaults to null on both platform modules.
`services.pynixd.metrics.enable` sets it, and these state what that reaches.

The route matters as much as the body. `enable_metrics` decides whether
`/metrics` is registered at all, and `metrics_no_auth` decides whether the
authentication middleware lets it past -- a metrics port that answers 401 to
a scraper is the same as no metrics port.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from aiohttp.test_utils import TestClient, TestServer
from prometheus_client import REGISTRY

from nix_daemon_protocol.ids import LOCAL_STORE_ID
from pynixd import metrics
from pynixd.config import LocalSocketStoreSpec
from pynixd.http_server import PynixdHttpServer
from pynixd.store.local_daemon import LocalStore

if TYPE_CHECKING:
    import pytest


def _server(*, enable_metrics: bool = True, enable_cache: bool = True) -> PynixdHttpServer:
    spec = LocalSocketStoreSpec(store_id=LOCAL_STORE_ID, monitor=False, use_db=False)
    return PynixdHttpServer(LocalStore(spec), enable_cache=enable_cache, enable_metrics=enable_metrics)


def _routes(server: PynixdHttpServer) -> set[str]:
    return {getattr(route.resource, "canonical", "") for route in server.app.router.routes()}


class TestTheRoute:
    def test_metrics_is_served_when_it_is_enabled(self) -> None:
        assert "/metrics" in _routes(_server(enable_metrics=True))

    def test_metrics_is_absent_when_it_is_not(self) -> None:
        """Not registered, rather than registered and refusing.

        A route that exists and answers 403 tells a scraper the endpoint is
        there and it is unwelcome. This one is simply not there.
        """
        assert "/metrics" not in _routes(_server(enable_metrics=False))

    def test_the_cache_can_be_off_while_metrics_is_on(self) -> None:
        """The combination `services.pynixd.metrics.enable` asks for.

        One HTTP server serves both, so turning on a metrics port turns on the
        binary cache unless something says otherwise. `http_enable_cache`
        defaults to true in `config.py`, and the module sets it false.
        """
        routes = _routes(_server(enable_metrics=True, enable_cache=False))
        assert "/metrics" in routes
        assert "/nix-cache-info" not in routes


class TestTheBody:
    async def test_metrics_answers_a_prometheus_exposition(self) -> None:
        response = await _server(enable_metrics=True).handle_metrics(None)  # type: ignore[arg-type]  # the handler does not read the request

        assert response.status == 200
        assert response.content_type.startswith("text/plain")
        assert response.body is not None

    async def test_the_body_names_pynixd_s_own_metrics(self) -> None:
        """A registry that answered only the process collectors would pass a
        check that reads the status alone."""
        response = await _server(enable_metrics=True).handle_metrics(None)  # type: ignore[arg-type]  # the handler does not read the request
        # `body` is typed as a `Payload` too, and this route always answers
        # bytes. Narrow it rather than cast: a Payload here would be a change
        # worth failing on.
        assert isinstance(response.body, bytes | bytearray)
        body = bytes(response.body).decode()

        assert "pynixd_build_queue_size" in body
        assert "pynixd_builds_completed_total" in body


class TestRequestCounting:
    """The middleware labels by the route pattern, never by the path.

    A binary cache is asked for one path per store path it serves, so a label
    carrying `request.path` would give one series per path in the store. That
    is the failure mode that takes a Prometheus down, and it looks fine on the
    day it ships.
    """

    def _requests(self, route: str, method: str, status: str) -> float:
        got = REGISTRY.get_sample_value(
            "pynixd_http_requests_total",
            {"route": route, "method": method, "status": status},
        )
        return 0.0 if got is None else got

    async def test_a_served_request_counts_under_its_route(self) -> None:
        server = _server(enable_metrics=True)
        before = self._requests("/healthz", "GET", "200")

        async with TestClient(TestServer(server.app)) as client:
            response = await client.get("/healthz")
            assert response.status == 200

        assert self._requests("/healthz", "GET", "200") - before == 1

    async def test_a_request_that_matches_no_route_counts_as_one_series(self) -> None:
        """A probe for `/wp-login.php` must not add a series to the registry."""
        server = _server(enable_metrics=True)
        before = self._requests("unmatched", "GET", "404")

        async with TestClient(TestServer(server.app)) as client:
            for path in ("/wp-login.php", "/.env", "/admin"):
                assert (await client.get(path)).status == 404

        assert self._requests("unmatched", "GET", "404") - before == 3

    def test_the_cache_routes_are_patterns(self) -> None:
        """What the middleware reads is `resource.canonical`, so this is the
        label a narinfo request produces."""
        assert "/{hash}.narinfo" in _routes(_server(enable_cache=True))


class TestStoreSpace:
    """The store's file system size, which `StoreSpaceCollector` reads.

    `nix/common.nix` turns the endpoint on; this is the number a node wants
    from it. The collector reads the file system and not the store's own
    record of itself, and the docstring there holds the two measurements that
    decided it.
    """

    def _series(self) -> dict[str, float]:
        body, _ = metrics.get_metrics_response()
        return {
            line.split(" ")[0]: float(line.split(" ")[1])
            for line in body.decode().splitlines()
            if line and not line.startswith("#") and "filesystem" in line
        }

    def test_both_numbers_are_served(self) -> None:
        series = self._series()
        assert "pynixd_store_filesystem_size_bytes" in series
        assert "pynixd_store_filesystem_available_bytes" in series

    def test_the_numbers_describe_a_real_file_system(self) -> None:
        """A collector that answered 0 for both would pass a check that only
        reads the names."""
        series = self._series()
        total = series["pynixd_store_filesystem_size_bytes"]
        available = series["pynixd_store_filesystem_available_bytes"]

        assert total > 0
        assert 0 <= available <= total

    def test_an_unreadable_store_directory_serves_no_series(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A store that is gone must not fail the scrape.

        Every other metric in the registry is still an answer, so the two
        series go absent rather than taking the rest with them.
        """
        monkeypatch.setattr(metrics, "real_store_dir", lambda: "/nonexistent-store-dir")

        assert self._series() == {}
