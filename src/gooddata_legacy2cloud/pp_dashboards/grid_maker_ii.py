# (C) 2026 GoodData Corporation
from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

# ----------------------- Config -----------------------


@dataclass
class GridConfig:
    canvas_width_px: int = 940
    columns: int = 12
    gutter_x_px: int = 16
    gutter_y_px: int = 16
    row_unit_px: int = 24
    rounding: str = "nearest"  # 'nearest'|'floor'|'ceil'

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
    # raw key: 'reportItem' / 'headlineItem' / 'iframeItem' / 'textItem' / 'filterItem' ...
    raw_kind: Optional[str]
    x: float
    y: float
    w: float
    h: float
    payload: Dict[str, Any]  # the inner dict


def iter_geometries(items: Iterable[Dict[str, Any]]) -> Iterable[ItemGeom]:
    for i, obj in enumerate(items):
        if not isinstance(obj, dict):
            continue
        inner_key, inner = next(
            ((k, v) for k, v in obj.items() if isinstance(v, dict)), (None, None)
        )
        if not inner:
            continue
        try:
            yield ItemGeom(
                idx=i,
                raw_kind=inner_key,
                x=float(inner["positionX"]),
                y=float(inner["positionY"]),
                w=float(inner["sizeX"]),
                h=float(inner["sizeY"]),
                payload=inner,
            )
        except KeyError:
            continue


# ----------------------- Type inference -----------------------

# We’ll map raw items into semantic types used for sizing/ordering
#   'filter' | 'text' | 'embed' | 'kpi' | 'kpiBig' | 'donut' | 'chart' | 'table' | 'other'

_KW_TABLE = re.compile(r"\b(table|grid)\b", re.I)
_KW_DONUT = re.compile(r"\b(donut|pie|ring)\b", re.I)
_KW_CHART = re.compile(r"\b(chart|line|area|bar|stacked)\b", re.I)
_KW_KPI = re.compile(r"\b(kpi|number|one\s*number|orders|sold|total)\b", re.I)


def infer_semantic_kind(it: ItemGeom) -> str:
    k = (it.raw_kind or "").lower()
    p = it.payload
    title = str(p.get("title") or p.get("text") or "").strip()

    if k == "filteritem":
        return "filter"
    if k == "textitem":
        return "text"
    if k == "iframeitem":
        return "embed"
    if k == "headlineitem":
        # GoodData "headline" is a number/KPI
        return "kpi"

    if k == "reportitem":
        viz = p.get("visualization", {})
        if isinstance(viz, dict):
            # If explicit viz keys exist, prefer them
            keys = {kk.lower() for kk in viz.keys()}
            if "grid" in keys:
                # treat as table if grid present
                return "table"
            if keys & {"donut", "pie", "ring"}:
                return "donut"
            if keys & {"area", "line", "bar", "column", "stackedarea", "stackedcolumn"}:
                return "chart"
            if "onenumber" in keys:
                return "kpiBig"

        # Fall back to title heuristics
        if _KW_TABLE.search(title):
            return "table"
        if _KW_DONUT.search(title):
            return "donut"
        if _KW_CHART.search(title):
            return "chart"
        if _KW_KPI.search(title):
            return "kpiBig"

    return "other"


# ----------------------- Sections (bands) -----------------------


def group_into_sections(
    geoms: List[ItemGeom],
    cfg: GridConfig,
    band_factor: float = 1.2,
    section_index_start: int = 1,
) -> List[Tuple[int, List[ItemGeom]]]:
    if not geoms:
        return []
    geoms = sorted(geoms, key=lambda g: (g.y, g.x))
    threshold = cfg.row_period_px * band_factor

    sections: List[Tuple[int, List[ItemGeom]]] = []
    current: List[ItemGeom] = [geoms[0]]
    last_y = geoms[0].y
    sec_no = section_index_start

    for g in geoms[1:]:
        if abs(g.y - last_y) <= threshold:
            current.append(g)
        else:
            current.sort(key=lambda gg: gg.x)
            sections.append((sec_no, current))
            sec_no += 1
            current = [g]
        last_y = g.y

    current.sort(key=lambda gg: gg.x)
    sections.append((sec_no, current))
    return sections


# ----------------------- Canonical sizes -----------------------

# Strong, opinionated default sizes (gridWidth, gridHeight).
# Tweak to your taste.
CANONICAL_HARD = {
    "text": (12, 2),
    "filter": (12, 2),
    "embed": (12, 6),
    "kpi": (3, 3),
    "kpiBig": (4, 9),
    "donut": (5, 10),  # <<— make donuts big & readable
    "chart": (8, 9),
    "table": (6, 14),  # tall tables
}


def apply_canonical(
    col_span: int, row_span: int, sem_kind: str, hard: bool = True
) -> Tuple[int, int]:
    if sem_kind not in CANONICAL_HARD:
        return col_span, row_span
    cw, rh = CANONICAL_HARD[sem_kind]
    if hard:
        return cw, rh
    # soft: nudge if "close"
    if abs(col_span - cw) <= 1:
        col_span = cw
    if abs(row_span - rh) <= 2:
        row_span = rh
    return col_span, row_span


# ----------------------- Section reordering -----------------------

# Lower priority appears HIGHER on the page.
SECTION_PRIORITY = {
    # headers/filters first, then charts/KPIs/donuts, then tables last
    "filter": 0,
    "text": 0,
    "embed": 0,
    "kpi": 1,
    "kpiBig": 1,
    "donut": 1,
    "chart": 1,
    "other": 1,
    "table": 2,
}


def section_priority(sem_kinds: List[str]) -> int:
    # Use the DOMINANT kind in the section (by count, then by area if you wish)
    counts = Counter(sem_kinds)
    dom = max(counts.items(), key=lambda kv: (kv[1], -SECTION_PRIORITY.get(kv[0], 1)))[
        0
    ]
    return SECTION_PRIORITY.get(dom, 1)


# ----------------------- Main API -----------------------


def migrate_to_sections(
    items: List[Dict[str, Any]],
    cfg: Optional[GridConfig] = None,
    band_factor: float = 1.2,
    section_index_start: int = 1,
    equalize_section_heights: bool = True,
    hard_canonical: bool = True,  # <<— force strong canonical sizes
) -> List[Dict[str, Any]]:
    """
    Output shape per item:
      {
        "size": {"xl": {"gridWidth": W, "gridHeight": H}},
        "section": S,
        "orderInSection": i,   # left→right as in original
        ... (url/title if present)
      }
    """
    cfg = cfg or GridConfig(rounding="ceil")  # bias up to avoid tiny donuts
    geoms = list(iter_geometries(items))
    sections = group_into_sections(geoms, cfg, band_factor, section_index_start)

    # 1) compute semantic kinds for all items
    sec_info: List[Tuple[int, List[Tuple[ItemGeom, str]]]] = []
    for sec_no, geos in sections:
        pairs = [(g, infer_semantic_kind(g)) for g in geos]
        sec_info.append((sec_no, pairs))

    # 2) reorder sections: filters/text/embed first, viz next, tables last (stable inside group)
    decorated = []
    for original_idx, (sec_no, pairs) in enumerate(sec_info):
        kinds = [k for (_g, k) in pairs]
        prio = section_priority(kinds)
        decorated.append((prio, original_idx, sec_no, pairs))
    decorated.sort(key=lambda t: t[1])  # by priority, then original order

    # 3) size transform + (optional) equalize height within each section
    out: List[Dict[str, Any]] = []
    new_sec_no = section_index_start
    for _, _orig_idx, _old_sec, pairs in decorated:
        # compute spans
        computed: List[Tuple[ItemGeom, str, int, int]] = []
        for g, sem_kind in pairs:
            gw = cfg.span_from_pixels(g.w, "x")
            gw = max(1, min(cfg.columns, gw))
            gh = cfg.span_from_pixels(g.h, "y")
            # hard or soft canonical
            gw, gh = apply_canonical(gw, gh, sem_kind, hard=hard_canonical)
            gw = max(1, min(cfg.columns, gw))
            computed.append((g, sem_kind, gw, gh))

        # equalize heights to the tallest in this section
        if equalize_section_heights:
            sec_h = max((gh for _g, _kind, _gw, gh in computed), default=1)
        else:
            sec_h = None

        # emit in left→right order with orderInSection
        for order_i, (g, sem_kind, gw, gh) in enumerate(computed):
            final_h = sec_h if sec_h is not None else gh
            obj = {
                "size": {"xl": {"gridWidth": int(gw), "gridHeight": int(final_h)}},
                "section": int(new_sec_no) - 1,
                # "orderInSection": int(order_i),
                "semantic": sem_kind,  # helpful for debugging / QA; drop if you don't want it
            }
            out.append(obj)

        new_sec_no += 1

    return out


# ----------------------- Example -----------------------
if __name__ == "__main__":
    sample = [
        {
            "reportItem": {
                "drills": [
                    {
                        "definition": {
                            "drillToAttributeDFs": {
                                "fromAttributes": [
                                    "/gdc/md/fkxyvp08rrrkfqss1ai656hvs0m77vl0/obj/582"
                                ],
                                "fromMetrics": [],
                                "toDisplayForms": [
                                    "/gdc/md/fkxyvp08rrrkfqss1ai656hvs0m77vl0/obj/634"
                                ],
                            }
                        },
                        "target": "pop-up",
                    }
                ],
                "filters": [
                    "fr-yui_3_14_1_1_1754900049155_40179",
                    "fr-yui_3_14_1_1_1754900049155_42576",
                    "fr-yui_3_14_1_1_1755168630550_62761",
                    "fr-yui_3_14_1_1_1755168630550_66079",
                    "fr-yui_3_14_1_1_1755168630550_73932",
                    "fr-yui_3_14_1_1_1755168630550_79082",
                    "fr-yui_3_14_1_1_1755168630550_85804",
                ],
                "obj": "/gdc/md/fkxyvp08rrrkfqss1ai656hvs0m77vl0/obj/4017",
                "positionX": 0,
                "positionY": 520,
                "sizeX": 380,
                "sizeY": 680,
                "style": {"background": {"opacity": 0.0}, "displayTitle": 1},
                "visualization": {
                    "grid": {
                        "columnWidths": [
                            {
                                "locator": [
                                    {
                                        "attributeHeaderLocator": {
                                            "uri": "/gdc/md/fkxyvp08rrrkfqss1ai656hvs0m77vl0/obj/582"
                                        }
                                    }
                                ],
                                "width": 88,
                            },
                            {
                                "locator": [
                                    {
                                        "metricLocator": {
                                            "uri": "/gdc/md/fkxyvp08rrrkfqss1ai656hvs0m77vl0/obj/854"
                                        }
                                    }
                                ],
                                "width": 273,
                            },
                        ]
                    },
                    "oneNumber": {"labels": {}},
                },
            }
        },
        {
            "reportItem": {
                "filters": [
                    "fr-yui_3_14_1_1_1755168630550_62761",
                    "fr-yui_3_14_1_1_1755168630550_66079",
                    "fr-yui_3_14_1_1_1755168630550_73932",
                    "fr-yui_3_14_1_1_1755168630550_79082",
                ],
                "obj": "/gdc/md/fkxyvp08rrrkfqss1ai656hvs0m77vl0/obj/4121",
                "positionX": 400,
                "positionY": 540,
                "sizeX": 540,
                "sizeY": 660,
                "style": {"background": {"opacity": 0.0}, "displayTitle": 0},
                "visualization": {
                    "grid": {"columnWidths": []},
                    "oneNumber": {"labels": {}},
                },
            }
        },
        {
            "reportItem": {
                "filters": [
                    "fr-yui_3_14_1_1_1755168630550_62761",
                    "fr-yui_3_14_1_1_1755168630550_66079",
                    "fr-yui_3_14_1_1_1755168630550_73932",
                    "fr-yui_3_14_1_1_1755168630550_79082",
                    "fr-yui_3_14_1_1_1755168630550_85804",
                ],
                "obj": "/gdc/md/fkxyvp08rrrkfqss1ai656hvs0m77vl0/obj/4199",
                "positionX": 550,
                "positionY": 180,
                "sizeX": 390,
                "sizeY": 340,
                "style": {"background": {"opacity": 0.0}, "displayTitle": 0},
                "visualization": {
                    "grid": {"columnWidths": []},
                    "oneNumber": {"labels": {}},
                },
            }
        },
        {
            "headlineItem": {
                "filterAttributeDF": "/gdc/md/fkxyvp08rrrkfqss1ai656hvs0m77vl0/obj/286",
                "filters": [
                    "fr-yui_3_14_1_1_1754900049155_40179",
                    "fr-yui_3_14_1_1_1754900049155_42576",
                    "fr-yui_3_14_1_1_1755168630550_62761",
                    "fr-yui_3_14_1_1_1755168630550_66079",
                    "fr-yui_3_14_1_1_1755168630550_73932",
                    "fr-yui_3_14_1_1_1755168630550_79082",
                    "fr-yui_3_14_1_1_1755168630550_85804",
                ],
                "linkedWithExternalFilter": 1,
                "metric": "/gdc/md/fkxyvp08rrrkfqss1ai656hvs0m77vl0/obj/797",
                "positionX": 690,
                "positionY": 320,
                "sizeX": 110,
                "sizeY": 50,
                "style": {"background": {"opacity": 0.0}, "displayTitle": 1},
                "title": "Title",
            }
        },
        {
            "headlineItem": {
                "filterAttributeDF": "/gdc/md/fkxyvp08rrrkfqss1ai656hvs0m77vl0/obj/286",
                "filters": [
                    "fr-yui_3_14_1_1_1754900049155_40179",
                    "fr-yui_3_14_1_1_1754900049155_42576",
                    "fr-yui_3_14_1_1_1755168630550_62761",
                    "fr-yui_3_14_1_1_1755168630550_66079",
                    "fr-yui_3_14_1_1_1755168630550_73932",
                    "fr-yui_3_14_1_1_1755168630550_79082",
                    "fr-yui_3_14_1_1_1755168630550_85804",
                ],
                "linkedWithExternalFilter": 1,
                "metric": "/gdc/md/fkxyvp08rrrkfqss1ai656hvs0m77vl0/obj/2570",
                "positionX": 430,
                "positionY": 180,
                "sizeX": 110,
                "sizeY": 50,
                "style": {"background": {"opacity": 0.0}, "displayTitle": 1},
                "title": "ORDERS",
            }
        },
        {
            "reportItem": {
                "filters": [
                    "fr-yui_3_14_1_1_1754900049155_40179",
                    "fr-yui_3_14_1_1_1754900049155_42576",
                    "fr-yui_3_14_1_1_1755168630550_62761",
                    "fr-yui_3_14_1_1_1755168630550_66079",
                    "fr-yui_3_14_1_1_1755168630550_73932",
                    "fr-yui_3_14_1_1_1755168630550_79082",
                    "fr-yui_3_14_1_1_1755168630550_85804",
                ],
                "obj": "/gdc/md/fkxyvp08rrrkfqss1ai656hvs0m77vl0/obj/4269",
                "positionX": 0,
                "positionY": 290,
                "sizeX": 540,
                "sizeY": 220,
                "style": {"background": {"opacity": 0.0}, "displayTitle": 0},
                "visualization": {
                    "grid": {"columnWidths": []},
                    "oneNumber": {"labels": {}},
                },
            }
        },
    ]

    cfg = GridConfig(
        canvas_width_px=940,
        columns=12,
        gutter_x_px=24,
        gutter_y_px=16,
        row_unit_px=5,
        rounding="nearest",
    )

    res = migrate_to_sections(
        sample, cfg, equalize_section_heights=True, hard_canonical=False
    )

    from pprint import pprint

    pprint(res)
