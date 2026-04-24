# (C) 2026 GoodData Corporation
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
import math


# ----------------------- Config -----------------------


@dataclass
class GridConfig:
    canvas_width_px: int = 940
    columns: int = 12
    gutter_x_px: int = 16
    gutter_y_px: int = 16
    row_unit_px: int = 24
    rounding: str = "nearest"  # 'nearest' | 'floor' | 'ceil'

    @property
    def col_unit_px(self) -> float:
        return (
            self.canvas_width_px - (self.columns - 1) * self.gutter_x_px
        ) / self.columns

    @property
    def col_period_px(self) -> float:
        return self.col_unit_px + self.gutter_x_px

    @property
    def row_period_px(self) -> float:
        return self.row_unit_px + self.gutter_y_px

    def span_from_pixels(self, px: float, axis: str) -> int:
        if axis == "x":
            unit, gutter = self.col_unit_px, self.gutter_x_px
        elif axis == "y":
            unit, gutter = self.row_unit_px, self.gutter_y_px
        else:
            raise ValueError("axis must be 'x' or 'y'")
        ratio = (px + gutter) / (unit + gutter)
        if self.rounding == "nearest":
            span = round(ratio)
        elif self.rounding == "floor":
            span = math.floor(ratio)
        elif self.rounding == "ceil":
            span = math.ceil(ratio)
        else:
            raise ValueError("rounding must be nearest|floor|ceil")
        return max(1, int(span))


# ----------------------- Parsing -----------------------


@dataclass
class ItemGeom:
    idx: int
    kind: Optional[str]  # top-level key if you care (headlineItem / iframeItem / ...)
    x: float
    y: float
    w: float
    h: float
    migration_id: Optional[str]  # carry original id if present


def iter_geometries(items: Iterable[Dict[str, Any]]) -> Iterable[ItemGeom]:
    """
    Extract (x,y,w,h) from each top-level dict, ignoring the exact key name.
    """
    for i, obj in enumerate(items):
        try:
            yield ItemGeom(
                idx=i,
                kind=obj["type"],
                x=float(obj["positionX"]),
                y=float(obj["positionY"]),
                w=float(obj["sizeX"]),
                h=float(obj["sizeY"]),
                migration_id=obj.get("migration_id", ""),
            )
        except KeyError:
            # skip items without complete geometry
            continue


# ----------------------- Sections (bands) -----------------------


def group_into_sections(
    geoms: List[ItemGeom],
    cfg: GridConfig,
    band_factor: float = 1.2,
    section_index_start: int = 1,
) -> List[Tuple[int, List[ItemGeom]]]:
    """
    Group items into horizontal sections by 'y'.
    Items whose y-difference is within (band_factor * row_period) belong to the same section.
    Returns a list of (section_number, [items_in_section_sorted_by_x])
    """
    if not geoms:
        return []

    geoms = sorted(geoms, key=lambda g: (g.y, g.x))
    threshold = cfg.row_period_px * band_factor

    sections: List[Tuple[int, List[ItemGeom]]] = []
    current: List[ItemGeom] = [geoms[0]]
    last_y = geoms[0].y
    section_no = section_index_start

    for g in geoms[1:]:
        if abs(g.y - last_y) <= threshold:
            current.append(g)
        else:
            current.sort(key=lambda gg: gg.x)
            sections.append((section_no, current))
            section_no += 1
            current = [g]
        last_y = g.y

    current.sort(key=lambda gg: gg.x)
    sections.append((section_no, current))
    return sections


# ----------------------- Size transform (+ optional type bias) -----------------------


def bias_towards_canonical(
    col_span: int,
    row_span: int,
    kind: Optional[str],
    canonical: Optional[Dict[str, Tuple[int, int]]] = None,
) -> Tuple[int, int]:
    """
    If `kind` has a canonical (w,h) and we're close, nudge to it.
    """
    if not canonical or not kind or kind not in canonical:
        return col_span, row_span
    canonical_width, canonical_height = canonical[kind]
    if abs(col_span - canonical_width) <= 1:
        col_span = canonical_width
    if abs(row_span - canonical_height) <= 2:
        row_span = canonical_height
    return col_span, row_span


# ----------------------- Main API -----------------------


def migrate_to_sections(
    items: List[Dict[str, Any]],
    cfg: Optional[GridConfig] = None,
    band_factor: float = 1.2,
    apply_canonical: bool = True,
    section_index_start: int = 0,
) -> List[Dict[str, Any]]:
    """
    Convert pixel-based items into:
      { "size": { "xl": { gridWidth, gridHeight }}, "section": N, ... }
    Section numbers are inferred from 'y' grouping; order inside a section is left-to-right.
    """
    cfg = cfg or GridConfig()
    geoms = list(iter_geometries(items))
    sections = group_into_sections(geoms, cfg, band_factor, section_index_start)

    canonical = (
        {
            # Tweak to taste or disable with apply_canonical=False
            "headlineItem": (3, 3),  # KPI-ish tiles
            "iframeItem": (12, 6),  # wide embeds by default
            # add other kinds if you have them
        }
        if apply_canonical
        else {}
    )

    out: List[Dict[str, Any]] = []

    for section_number, geometries in sections:
        for geometry in geometries:
            grid_width = cfg.span_from_pixels(geometry.w, "x")
            grid_width = max(1, min(cfg.columns, grid_width))
            grid_height = cfg.span_from_pixels(geometry.h, "y")
            if apply_canonical:
                grid_width, grid_height = bias_towards_canonical(
                    grid_width, grid_height, geometry.kind, canonical
                )
                grid_width = max(1, min(cfg.columns, grid_width))

            obj = {
                "size": {
                    "xl": {"gridWidth": int(grid_width), "gridHeight": int(grid_height)}
                },
                "section": int(section_number),
                "migration_id": geometry.migration_id,
            }
            # carry useful props
            out.append(obj)

    return out
