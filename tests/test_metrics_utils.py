# (C) 2026 GoodData Corporation
"""
Unit tests for gooddata_legacy2cloud.metrics.utils helper functions.
"""

import json
from types import SimpleNamespace

from gooddata_legacy2cloud.metrics.utils import (
    build_placeholder_maql,
    comment_out_lines,
    disable_broken_metric,
)


def test_comment_out_lines_prefixes_every_line():
    assert comment_out_lines("first line\nsecond line") == "#first line\n#second line"


def test_build_placeholder_maql_without_extra_message():
    result = build_placeholder_maql("SELECT COUNT([/gdc/md/project/obj/1])")

    assert result == (
        "#Failed MAQL:\n#SELECT COUNT([/gdc/md/project/obj/1])\n\nSELECT SQRT(-1)"
    )


def test_build_placeholder_maql_defaults_to_error_label():
    result = build_placeholder_maql(
        "SELECT SUM([/gdc/md/project/obj/2])", "something went wrong"
    )

    assert result == (
        "#Failed MAQL:\n#SELECT SUM([/gdc/md/project/obj/2])"
        "\n\n#Error:\n#something went wrong\n\nSELECT SQRT(-1)"
    )


def test_build_placeholder_maql_with_custom_label():
    result = build_placeholder_maql(
        "SELECT SUM([/gdc/md/project/obj/3])",
        "Cannot resolve reference",
        "API Error 400",
    )

    assert result == (
        "#Failed MAQL:\n#SELECT SUM([/gdc/md/project/obj/3])"
        "\n\n#API Error 400:\n#Cannot resolve reference\n\nSELECT SQRT(-1)"
    )


def test_build_placeholder_maql_with_explicit_no_label():
    result = build_placeholder_maql(
        "SELECT SUM([/gdc/md/project/obj/4])", "first line\nsecond line", None
    )

    assert result == (
        "#Failed MAQL:\n#SELECT SUM([/gdc/md/project/obj/4])"
        "\n\n#first line\n#second line\n\nSELECT SQRT(-1)"
    )


def test_disable_broken_metric_without_error_response():
    metric = {
        "data": {
            "attributes": {
                "title": "My metric",
                "tags": ["existing"],
                "content": {"maql": "SELECT SUM({fact/orders.amount})"},
            }
        }
    }

    result = disable_broken_metric(metric)

    attributes = result["data"]["attributes"]
    assert attributes["title"] == "[ERROR] My metric"
    assert attributes["tags"] == ["existing", "ERROR"]
    assert attributes["content"]["maql"] == (
        "#Failed MAQL:\n#SELECT SUM({fact/orders.amount})\n\nSELECT SQRT(-1)"
    )


def test_disable_broken_metric_with_no_existing_tags():
    metric = {
        "data": {
            "attributes": {
                "title": "My metric",
                "tags": None,
                "content": {"maql": "SELECT SUM({fact/orders.amount})"},
            }
        }
    }

    result = disable_broken_metric(metric)

    assert result["data"]["attributes"]["tags"] == ["ERROR"]


def test_disable_broken_metric_appends_api_error_detail():
    metric = {
        "data": {
            "attributes": {
                "title": "My metric",
                "tags": [],
                "content": {"maql": "SELECT SUM({fact/orders.amount})"},
            }
        }
    }
    error_response = SimpleNamespace(
        status_code=400,
        text=json.dumps({"detail": "Cannot resolve reference"}),
    )

    result = disable_broken_metric(metric, error_response)

    assert result["data"]["attributes"]["content"]["maql"] == (
        "#Failed MAQL:\n#SELECT SUM({fact/orders.amount})"
        "\n\n#API Error 400:\n#Cannot resolve reference\n\nSELECT SQRT(-1)"
    )


def test_disable_broken_metric_with_non_object_json_error_body():
    metric = {
        "data": {
            "attributes": {
                "title": "My metric",
                "tags": [],
                "content": {"maql": "SELECT SUM({fact/orders.amount})"},
            }
        }
    }
    error_response = SimpleNamespace(
        status_code=500,
        text=json.dumps(["Internal Server Error"]),
    )

    result = disable_broken_metric(metric, error_response)

    assert result["data"]["attributes"]["content"]["maql"] == (
        "#Failed MAQL:\n#SELECT SUM({fact/orders.amount})"
        '\n\n#API Error 500:\n#["Internal Server Error"]\n\nSELECT SQRT(-1)'
    )
