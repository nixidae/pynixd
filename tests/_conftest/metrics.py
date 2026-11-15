"""What pynixd's Prometheus metrics say, read in the test's own process."""

from __future__ import annotations

from prometheus_client import REGISTRY


def fallthroughs(op: str) -> float:
    """How often pynixd sent *op* to the daemon after all (`pynixd_op_fallthrough_total`), over every store."""
    return sum(
        sample.value
        for metric in REGISTRY.collect()
        if metric.name == "pynixd_op_fallthrough"
        for sample in metric.samples
        if sample.name == "pynixd_op_fallthrough_total" and sample.labels.get("op") == op
    )
