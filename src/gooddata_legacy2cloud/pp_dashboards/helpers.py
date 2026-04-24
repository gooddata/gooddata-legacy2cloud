# (C) 2026 GoodData Corporation
"""Helper functions for pixel perfect dashboard migration."""

import concurrent.futures
import logging
import os
from typing import Any

from gooddata_legacy2cloud.pp_dashboards.data_classes import PPDashboardContext

logger = logging.getLogger("migration")


def extract_values_by_key(data: Any, key: str) -> list[str]:
    """Extract all values of keys with given name from any depth within a nested structure.

    Traverses dictionaries, lists, and tuples.
    Returns a flat list[str]. If the matched value is a list/tuple of strings, it is unwrapped
    and extended.

    Args:
        data: The nested data structure to search (dict, list, tuple, or any combination)
        key: The key name to search for

    Returns:
        A list of string values found for the specified key at any depth
    """
    results: list[str] = []
    stack: list[Any] = [data]

    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            for k, v in current.items():
                if k == key:
                    if isinstance(v, str):
                        results.append(v)
                    elif isinstance(v, (list, tuple)) and all(
                        isinstance(x, str) for x in v
                    ):
                        results.extend(x for x in v if isinstance(x, str))
                if isinstance(v, (dict, list, tuple)):
                    stack.append(v)
        elif isinstance(current, (list, tuple)):
            for item in current:
                if isinstance(item, (dict, list, tuple)):
                    stack.append(item)
    return results


def prefetch_legacy_objects(ctx: PPDashboardContext, obj_values: list[Any]) -> None:
    """Asynchronously prefetch Legacy objects to warm the cache.

    Uses ctx.legacy_client.get_object() to fetch objects concurrently.
    Accepts any values in obj_values; only strings and dicts are passed as obj_link.
    Errors are ignored, as this is a best-effort cache warming step.

    Args:
        ctx: Migration context containing Legacy API client
        obj_values: List of object URIs or references to prefetch
    """

    def _fetch(v: Any) -> None:
        try:
            ctx.legacy_client.get_object(obj_link=v)
        except Exception as e:
            # Debug-level noise only; do not interrupt migration flow
            logger.debug(f"Prefetch failed for obj_link={type(v)}: {e}")

    if not obj_values:
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = [
            executor.submit(_fetch, v) for v in obj_values if isinstance(v, (str, dict))
        ]
        # Wait for completion without processing results
        for _ in concurrent.futures.as_completed(futures):
            pass
