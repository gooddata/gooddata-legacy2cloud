# (C) 2026 GoodData Corporation
import re
from math import hypot
from typing import Any, Iterable, Optional, Tuple, TypeVar

from pydantic import BaseModel


class Meta(BaseModel):
    author: str
    category: str
    identifier: str
    title: str
    uri: str
    tags: str = ""
    unlisted: Optional[int] = 0


def sanitize_string(s: str) -> str:
    # 1. Remove disallowed chars
    allowed = re.sub(r"[^.A-Za-z0-9_-]", "", s)

    # 2. Remove leading dots (can be multiple)
    allowed = re.sub(r"^\.+", "", allowed)

    # 3. Trim to 255 chars
    return allowed[:255]


def get_migration_id(legacy_title, legacy_identifier, prefix="pp") -> str:
    return sanitize_string(
        f"{prefix}_{legacy_title.lower().replace(' ', '_')}_{legacy_identifier}"
    )


T = TypeVar("T", bound=Any)  # Item or subclasses


def nearest_item(
    target: T,
    items: Iterable[T],
    *,
    return_distance: bool = True,
    exclude_self: bool = True,
) -> Tuple[Optional[T], float] | Optional[T]:
    """
    Find the nearest Item (by Euclidean distance) to 'target' using positionX/positionY.

    Args:
        target: The reference item (must have positionX and positionY attributes).
        items: Iterable of candidate items to search through.
        return_distance: If True, returns (nearest_item, distance). If False, returns only the item.
        exclude_self: Exclude the target from candidates if it's present in 'items'.

    Returns:
        If return_distance=True: (nearest_item, distance). If no candidates, returns (None, inf).
        If return_distance=False: nearest_item or None if not found.
    """
    tx, ty = target.positionX, target.positionY

    best_obj: Optional[T] = None
    best_dist: float = float("inf")

    for obj in items:
        if exclude_self and obj is target:
            continue
        dx = obj.positionX - tx
        dy = obj.positionY - ty
        d = hypot(dx, dy)
        if d < best_dist:
            best_dist = d
            best_obj = obj

    if return_distance:
        return best_obj, best_dist
    return best_obj
