# (C) 2026 GoodData Corporation
ALL_TIME_DATE_FILTER = "all_time_date_filter"
# The ALL_TIME_DATE_FILTER is used as a placeholder for filter identifier
# in cases when date filter has 'from_' and 'to' attributes set to None. This
# can happen if the date filter is default (all time) and attribute filters
# have values selected. Legacy backend will then create the absolute date filter
# in the metadata, but it will not have from and to attributes. In that case,
# we need to signal that the default date filter should be applied.
