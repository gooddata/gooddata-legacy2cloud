# (C) 2026 GoodData Corporation
DATE_TYPES = [
    "GDC.time.year",
    "GDC.time.quarter_in_year",
    "GDC.time.quarter",
    "GDC.time.month",
    "GDC.time.month_in_year",
    "GDC.time.month_in_quarter",
    "GDC.time.week",
    "GDC.time.week_us",
    "GDC.time.week_in_year",
    "GDC.time.week_in_quarter",
    "GDC.time.euweek_in_year",
    "GDC.time.euweek_in_quarter",
    "GDC.time.date",
    "GDC.time.day",
    "GDC.time.day_eu",
    "GDC.time.day_iso",
    "GDC.time.day_us",
    "GDC.time.day_us_long",
    "GDC.time.day_us_noleading",
    "GDC.time.day_in_year",
    "GDC.time.day_in_quarter",
    "GDC.time.day_in_month",
    "GDC.time.day_in_week",
    "GDC.time.day_in_euweek",
]

DAY_PARSING_MASKS = {
    "date.day.us.mm_dd_yyyy": "%m/%d/%Y",  # e.g. 02/20/2019
    "date.day.eu.dd_mm_yyyy": "%d-%m-%Y",
    "date.day.uk.dd_mm_yyyy": "%d/%m/%Y",
    "date.day.yyyy_mm_dd": "%Y-%m-%d",
    "date.mmddyyyy": "%m/%d/%Y",
    "date.yyyymmdd": "%Y-%m-%d",
    "date.mdyy": "%m/%d/%Y",
    "date.ddmmyyyy": "%d/%m/%Y",
    "date.eddmmyyyy": "%d-%m-%Y",
}

MONTH_YEAR_PARSING_MASKS = {
    "act81lMifn6q": "%b %Y",  # e.g. Jan 2021
    "acx81lMifn6q": "%B %Y",  # e.g. January 2021
    "acv81lMifn6q": "%m/%Y",  # e.g. 2/2019
}

MONTH_PARSING_MASK = {
    "month.in.year.short": "%b",  # e.g. Jan
    "abm81lMifn6q": "%b",
}

DAY_OF_WEEK_PARSING_MASKS = {
    "day.in.week.short": "%a",  # e.g. Mon
    "abK81lMifn6q": "%a",  # e.g. Mon
}

PARSING_MASKS = {
    **DAY_PARSING_MASKS,
    **MONTH_YEAR_PARSING_MASKS,
    **MONTH_PARSING_MASK,
    **DAY_OF_WEEK_PARSING_MASKS,
}

DAY_SHORTCUTS = {
    "Mon": "00",
    "Tue": "01",
    "Wed": "02",
    "Thu": "03",
    "Fri": "04",
    "Sat": "05",
    "Sun": "06",
}

YEAR_IDs = ["aag81lMifn6q"]  # e.g. 2019

QUARTER_IDs = ["aci81lMifn6q"]  # e.g. Q1/2019

WEEK_IDs = ["aa281lMifn6q", "aaA81lMifn6q"]  # e.g.W2/1900  # e.g.W2/1900

MISSING_VALUE = "--MISSING VALUE--"
DELETED_VALUE = "--DELETED VALUE--"

GDC_TIME_DATE = "GDC.time.date"

LEGACY_NULL_DATE = "1-01-01"
