# (C) 2026 GoodData Corporation
from gooddata_legacy2cloud.constants import TIME_MACROS
from gooddata_legacy2cloud.metrics.maql.constants import (
    CLOUD_TIME_GRANULARITIES,
    DEFAULT_GRANULARITY,
)


def get_linked_objects(content, parent=None):
    """
    Returns all the dependent objects in the expression.
    """
    expression_objs = []
    for item in content:
        if "content" in item:
            new_parent = {
                "value": item.get("value"),
                "type": item["type"],
                "parent": parent,
            }
            expression_objs.extend(get_linked_objects(item["content"], new_parent))
        else:
            item["parent"] = parent
            expression_objs.append(item)
    return expression_objs


def get_identifiers_from_tree(content):
    """
    Returns the identifiers in the nested expression.
    """
    identifiers = []
    for item in content:
        if "content" in item:
            nested_items = get_identifiers_from_tree(item["content"])
            identifiers.extend(nested_items)
        elif "value" in item:
            value = item["value"].replace("{", "").replace("}", "")
            identifiers.append(value)
    return identifiers


def get_time_granularity_from_identifier(identifier: str):
    """
    Checks if the identifier's suffix contains any string from Cloud time granularities.
    """
    for suffix in CLOUD_TIME_GRANULARITIES:
        if identifier.endswith(suffix):
            return suffix
    return None


def get_content_granularity(content):
    """
    Returns the granularity of the nested content.
    """
    identifiers = get_identifiers_from_tree(content)
    granularity = None
    has_time_macro = False
    for item in identifiers:
        # handle THIS, PREVIOUS, NEXT macros
        upper_item = item.upper()
        if upper_item.startswith(tuple(TIME_MACROS)):
            has_time_macro = True
            continue

        item_granularity = get_time_granularity_from_identifier(item)
        if item_granularity:
            granularity = item_granularity

    # handle the case with time macro
    if has_time_macro and not granularity:
        return DEFAULT_GRANULARITY

    return granularity


def get_datetime_diff_argument(content, granularity):
    value = content["value"]
    if value in TIME_MACROS:
        return f"{value}({granularity})"

    if content["type"] in ["number"]:
        return f'"{value}"'

    return value


def get_datetime_diff_expression(json_content, granularity):
    """
    Returns the DATETIME_DIFF expression for the content.
    """
    first_content = get_datetime_diff_argument(json_content[0], granularity)
    second_content = get_datetime_diff_argument(json_content[1], granularity)

    return f"DATETIME_DIFF({second_content}, {first_content}, {granularity})"
