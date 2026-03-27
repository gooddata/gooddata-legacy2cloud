# (C) 2026 GoodData Corporation
DATE_TYPES_MAPPINGS = {
    "GDC.time.year": "year",
    "GDC.time.quarter_in_year": "quarter",
    "GDC.time.quarter": "quarter",
    "GDC.time.month": "month",
    "GDC.time.month_in_year": "month",
    "GDC.time.month_in_quarter": "month",
    "GDC.time.week": "week",
    "GDC.time.week_us": "week",
    "GDC.time.week_in_year": "week",
    "GDC.time.week_in_quarter": "week",
    "GDC.time.euweek_in_year": "week",
    "GDC.time.euweek_in_quarter": "week",
    "GDC.time.date": "day",
    "GDC.time.day": "day",
    "GDC.time.day_eu": "day",
    "GDC.time.day_iso": "day",
    "GDC.time.day_us": "day",
    "GDC.time.day_us_long": "day",
    "GDC.time.day_us_noleading": "day",
    "GDC.time.day_in_year": "day",
    "GDC.time.day_in_quarter": "day",
    "GDC.time.day_in_month": "day",
    "GDC.time.day_in_week": "day",
    "GDC.time.day_in_euweek": "day",
}

DATE_NUMBERS_WITH_TWO_DIGITS = [
    "GDC.time.quarter_in_year",
    "GDC.time.month_in_year",
    "GDC.time.week_in_year",
    "GDC.time.euweek_in_year",
    "GDC.time.day_in_quarter",
    "GDC.time.day_in_month",
    "GDC.time.day_in_week",
    "GDC.time.day_in_euweek",
]

DATE_NUMBERS_WITH_THREE_DIGITS = ["GDC.time.day_in_year"]

RELATION_OPERATORS = ["=", "<", ">", "<=", ">="]

SHIFT_OPERATIONS = ["+", "-"]

CLOUD_TIME_GRANULARITIES = [
    "day",
    "week",
    "month",
    "quarter",
    "year",
    "dayOfWeek",
    "dayOfMonth",
    "dayOfYear",
    "weekOfYear",
    "monthOfYear",
    "quarterOfYear",
]

DEFAULT_GRANULARITY = "day"
