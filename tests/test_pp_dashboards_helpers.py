# (C) 2026 GoodData Corporation
"""
Unit tests for pixel perfect dashboard helper functions.
"""

# Import helper functions from the helpers module
from gooddata_legacy2cloud.pp_dashboards.helpers import extract_values_by_key


class TestExtractValuesByKey:
    """Tests for extract_values_by_key function."""

    def test_extract_from_simple_dict(self):
        """Test extraction from simple dictionary."""
        data = {"obj": "value1", "other": "value2"}
        result = extract_values_by_key(data, "obj")
        assert result == ["value1"]

    def test_extract_from_nested_dict(self):
        """Test extraction from nested dictionary."""
        data = {"level1": {"obj": "value1", "nested": {"obj": "value2"}}}
        result = extract_values_by_key(data, "obj")
        assert set(result) == {"value1", "value2"}

    def test_extract_from_list(self):
        """Test extraction when values are in lists."""
        data = {"items": [{"obj": "value1"}, {"obj": "value2"}]}
        result = extract_values_by_key(data, "obj")
        assert set(result) == {"value1", "value2"}

    def test_extract_list_values(self):
        """Test extraction when value itself is a list of strings."""
        data = {"obj": ["value1", "value2"]}
        result = extract_values_by_key(data, "obj")
        assert set(result) == {"value1", "value2"}

    def test_extract_from_complex_structure(self):
        """Test extraction from complex nested structure."""
        data = {
            "level1": {
                "obj": "value1",
                "items": [
                    {"obj": "value2", "nested": {"obj": "value3"}},
                    {"other": "ignore"},
                ],
            },
            "obj": ["value4", "value5"],
        }
        result = extract_values_by_key(data, "obj")
        assert set(result) == {"value1", "value2", "value3", "value4", "value5"}

    def test_extract_nonexistent_key(self):
        """Test extraction when key doesn't exist."""
        data = {"other": "value"}
        result = extract_values_by_key(data, "obj")
        assert result == []

    def test_extract_from_empty_dict(self):
        """Test extraction from empty dictionary."""
        data = {}
        result = extract_values_by_key(data, "obj")
        assert result == []

    def test_extract_with_tuple_value(self):
        """Test extraction when value is a tuple of strings."""
        data = {"obj": ("value1", "value2")}
        result = extract_values_by_key(data, "obj")
        assert set(result) == {"value1", "value2"}


# Note: Tests for _should_skip_or_overwrite and other functions will be added
# after Phase 2 refactoring when they are moved to the builder class
