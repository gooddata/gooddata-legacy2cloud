# (C) 2026 GoodData Corporation
"""
Verifies that metrics depending on computed attributes (directly or through
other metrics) are separated from plain ones.
"""

import copy

from gooddata_legacy2cloud.metrics.metrics_sorter import (
    split_metrics_by_computed_attribute,
)
from tests.test_utils import load_json

CA_METRIC_FILE = "tests/data/metrics/test_cases/with_computed_attribute_cloud.json"


def _metric(base: dict, metric_id: str, maql: str) -> dict:
    metric = copy.deepcopy(base)
    metric["data"]["id"] = metric_id
    metric["data"]["attributes"]["content"]["maql"] = maql
    return metric


def _ids(metrics: list[dict]) -> list[str]:
    return [m["data"]["id"] for m in metrics]


def test_transitive_dependency_on_computed_attribute() -> None:
    base = load_json(CA_METRIC_FILE)[0]
    direct = _metric(base, "a", "SELECT {computed_attribute/ca1}")
    indirect = _metric(base, "b", "SELECT SUM({metric/a})")
    deeper = _metric(base, "c", "SELECT {metric/b} + 1")
    unrelated = _metric(base, "d", "SELECT SUM({fact/f})")
    external = _metric(base, "e", "SELECT {metric/not_in_batch}")

    plain, on_ca = split_metrics_by_computed_attribute(
        [deeper, unrelated, indirect, direct, external]
    )

    assert _ids(plain) == ["d", "e"]
    assert _ids(on_ca) == ["c", "b", "a"]


def test_no_computed_attributes() -> None:
    base = load_json(CA_METRIC_FILE)[0]
    metrics = [_metric(base, "a", "SELECT 1"), _metric(base, "b", "{metric/a}")]

    plain, on_ca = split_metrics_by_computed_attribute(metrics)

    assert _ids(plain) == ["a", "b"]
    assert on_ca == []
