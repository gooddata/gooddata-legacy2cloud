# (C) 2025 GoodData Corporation
"""
Tests for Cloud Pydantic models used by migration scripts.

These tests focus on resilience to occasional inconsistencies in Cloud API
responses, to avoid hard-failing entire migrations when a single entity payload
contains unexpected nulls.
"""

from gooddata_platform2cloud.models.cloud.filter_context import FilterContextModel


def test_filter_context_model_allows_null_value_in_attribute_elements() -> None:
    """Ensure null entries inside attributeElements.values do not break parsing."""
    raw_filter_context = {
        "id": "filtercontext_test",
        "type": "filterContext",
        "attributes": {
            "title": "filterContext",
            "description": "",
            "content": {
                "filters": [
                    {
                        "attributeFilter": {
                            "localIdentifier": "lf_1",
                            "attributeElements": {"values": [None]},
                            "displayForm": {
                                "identifier": {"id": "attr.label", "type": "label"}
                            },
                            "negativeSelection": False,
                            "selectionMode": "multi",
                        }
                    }
                ],
                "version": 2,
            },
        },
    }

    model = FilterContextModel.model_validate(raw_filter_context)
    attribute_filter = model.attributes.content.filters[0].attribute_filter
    assert attribute_filter is not None
    assert attribute_filter.attribute_elements.values is None
