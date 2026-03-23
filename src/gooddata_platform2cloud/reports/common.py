# (C) 2026 GoodData Corporation
import hashlib


def generate_local_id(value: str) -> str:
    """Generate a consistent local identifier from a given string value using MD5."""
    return hashlib.md5(value.encode("utf-8")).hexdigest()


def contains_node_type(node, target_type: str) -> bool:
    """Recursively check whether any node in the given tree has a 'type' equal to target_type."""
    if isinstance(node, dict):
        if node.get("type") == target_type:
            return True
        for child in node.get("content", []):
            if contains_node_type(child, target_type):
                return True
    elif isinstance(node, list):
        for item in node:
            if contains_node_type(item, target_type):
                return True
    return False


def find_display_form(filter_attr_uri: str, column_attributes: list) -> dict:
    """
    Given an attribute URI and a list of column attributes, return the display form
    (an identifier object) for the first column attribute whose URI starts with the given URI.
    """
    for col_item in column_attributes:
        attr = col_item.get("attribute", {})
        col_uri = attr.get("uri", "")
        if not col_uri:
            continue
        if col_uri.startswith(filter_attr_uri + ".") or col_uri == filter_attr_uri:
            return {"id": col_uri, "type": "label"}
    return {"id": filter_attr_uri, "type": "label"}


def find_metric_object_value(node):
    """
    Recursively search the tree for a node with type 'metric object' and return its 'value'.
    """
    if isinstance(node, dict):
        if node.get("type") == "metric object":
            return node.get("value")
        for child in node.get("content", []):
            result = find_metric_object_value(child)
            if result:
                return result
    elif isinstance(node, list):
        for item in node:
            result = find_metric_object_value(item)
            if result:
                return result
    return None


def find_number_in_tree(node):
    """
    Recursively search the tree for a node with type 'number' and return its numeric value as a float.
    """
    if isinstance(node, dict):
        if node.get("type") == "number":
            try:
                return float(node.get("value", 0))
            except ValueError, TypeError:
                return 0
        for child in node.get("content", []):
            result = find_number_in_tree(child)
            if result is not None:
                return result
    elif isinstance(node, list):
        for item in node:
            result = find_number_in_tree(item)
            if result is not None:
                return result
    return None


def check_granularity(node):
    """
    Recursively check if any node in the tree has type 'by' or 'all other'.
    """
    if isinstance(node, dict):
        if node.get("type") in ["by", "all other"]:
            return True
        for child in node.get("content", []):
            if check_granularity(child):
                return True
    elif isinstance(node, list):
        for item in node:
            if check_granularity(item):
                return True
    return False
