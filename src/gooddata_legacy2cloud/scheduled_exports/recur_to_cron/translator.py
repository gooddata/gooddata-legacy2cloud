# (C) 2026 GoodData Corporation
"""
This module contains a class that converts basic cases of Date::Manip::Recur format
to a 6-field cron expression.

You can find the Date::Manip::Recur format documentation here:
https://metacpan.org/release/SBECK/Date-Manip-6.59/view/lib/Date/Manip/Recur.pm
"""


class RecurToCronError(Exception):
    """Exception raised when a Date::Manip::Recur format is invalid."""


class RecurToCronTranslator:
    """
    Class that converts Date::Manip::Recur frequency notation to a 6-field cron expression.
    Supports only simple cases and diligently raises exceptions when more complex cases
    are encountered.

    This class will only process Date::Manip::Recur instances where the asterisk
    is used as a separator. Asterisk values within fields are not currently supported.

    Read the Date::Manip::Recur format documentation for more details:
    https://metacpan.org/release/SBECK/Date-Manip-6.59/view/lib/Date/Manip/Recur.pm
    """

    def convert_date_manip_to_cron(self, recur: str) -> str:
        """
        Converts Date::Manip::Recur frequency notation to a 6-field cron expression.

        Only handles simple scheduling patterns that can be reliably converted.
        Raises errors for complex patterns to avoid incorrect translations.

        Args:
            date_manip: Date::Manip format string like "0:0:1*1:8:0:0"
                        Format: Y:M:W:D:H:MN:S (year:month:week:day:hour:minute:second)
                        One colon can be replaced with asterisk as separator.

        Returns:
            6-field cron string: "second minute hour day-of-month month day-of-week"

        Raises:
            ValueError: For invalid format or unsupported scheduling patterns.
        """

        if recur == "":
            raise RecurToCronError("Cannot convert empty string to cron expression.")

        # Strip whitespace and newlines
        recur_str = recur.strip()

        asterisk_count = recur_str.count("*")
        colon_count = recur_str.count(":")

        # If there is more than 1 asterisk, it means that Recur is invalid.
        if asterisk_count > 1:
            raise RecurToCronError(
                f"Invalid Date::Manip::Recur format: {recur_str}. Recur can only"
                " contain one asterisk."
            )

        # Date::Manip::Recur can use special characters, but we're not attempting to process those
        if any(char not in "0123456789*:" for char in recur_str):
            raise RecurToCronError(
                f"Unsupported Date::Manip::Recur format: {recur_str} This "
                "converter does not support characters other than numbers, "
                "asterisks, and colons."
            )

        # If there are 6 colons, it means that the asterisk is not used as a separator,
        # but as a part of the field value.
        if colon_count == 6 and asterisk_count > 0:
            raise RecurToCronError(
                f"Too complex Date::Manip::Recur format: {recur_str} This converter "
                "does not support prepended asterisk. Only asterisk used as a "
                "separator is currently accepted."
            )

        # Parse raw fields BEFORE preprocessing for pattern detection
        if asterisk_count == 1:
            interval_part_string = recur_str.split("*")[0]
            recurrence_time_string = recur_str.split("*")[1]
        elif asterisk_count == 0:
            interval_part_string = recur_str
            recurrence_time_string = ""

        raw_parts = f"{interval_part_string}:{recurrence_time_string}".split(":")

        if len(raw_parts) != 7:
            raise RecurToCronError(
                f"Invalid Date::Manip::Recur format: {recur_str} This converter "
                "requires 7 colon-separated fields (Y:M:W:D:H:MN:S). Only 1 colon "
                "can be replaced with asterisk."
            )

        # Extract raw values for pattern detection (before preprocessing)
        (
            _raw_year,
            raw_month,
            raw_week,
            raw_day,
            _raw_hour,
            _raw_minute,
            _raw_second,
        ) = raw_parts

        # Processing values: Convert interval values to cron format
        #
        interval_parts = interval_part_string.split(":")
        for i, part in enumerate(interval_parts):
            # Convert 0 to "*" (cron wildcard for "any value")
            if part == "0":
                interval_parts[i] = "*"
            # Convert 1 to "*" (every 1 = every, so use wildcard)
            elif part == "1":
                interval_parts[i] = "*"
            # Convert N to "*/N" (cron interval notation)
            elif part.isnumeric():
                interval_parts[i] = f"*/{part}"

        interval_part_string = ":".join(interval_parts)
        date_manip_without_asterisk = f"{interval_part_string}:{recurrence_time_string}"
        processed_parts = date_manip_without_asterisk.split(":")

        # Date::Manip::Recur input order: Y:M:W:D:H:MN:S (processed values)
        (
            processed_year,
            processed_month,
            processed_week,
            processed_day,
            processed_hour,
            processed_minute,
            processed_second,
        ) = processed_parts

        # Additional validation for complex patterns that should be rejected
        # Check for negative values, ranges, and complex modifiers
        for field_name, field_value in [
            ("year", processed_year),
            ("month", processed_month),
            ("week", processed_week),
            ("day", processed_day),
            ("hour", processed_hour),
            ("minute", processed_minute),
            ("second", processed_second),
        ]:
            if field_value and field_value != "*":
                # Check for negative values (only supported in limited cases we don't handle)
                if "-" in field_value and field_value != "0":
                    raise RecurToCronError(
                        f"Negative values not supported: {field_name}='{field_value}' in {recur_str}. "
                        "This converter only supports positive integer values."
                    )
                # Check for ranges (comma-separated or dash-separated lists)
                if "," in field_value or (
                    "-" in field_value
                    and field_value.count("-") >= 1
                    and field_value != "0"
                ):
                    raise RecurToCronError(
                        f"Range values not supported: {field_name}='{field_value}' in {recur_str}. "
                        "This converter only supports single integer values."
                    )

        # Parse fields based on processed values
        cron_second = self._parse_field(processed_second, 0, 59, "second")
        cron_minute = self._parse_field(processed_minute, 0, 59, "minute")
        cron_hour = self._parse_field(processed_hour, 0, 23, "hour")
        cron_month = self._parse_field(processed_month, 1, 12, "month")

        # PATTERN RECOGNITION: Check month/week values to determine schedule type
        # Based on analysis of real scheduling data, we found these patterns:

        if raw_month == "0" and raw_week == "0":
            # DAILY: month=0, week=0 means "every day"
            # Example: "0:0:0:1*11:0:0" = daily at 11:00
            # Cron: both day fields set to "*" (any day)
            cron_day_of_month = "*"
            cron_day_of_week = "*"

        elif raw_month == "0" and raw_week == "1":
            # WEEKLY: month=0, week=1 means "every week"
            # Example: "0:0:1*1:8:0:0" = every Monday at 8:00
            # Day field specifies which weekday (1=Monday, 7=Sunday)
            if raw_day == "0" or raw_day == "":
                raise RecurToCronError(
                    f"Weekly pattern missing day: {recur_str}. "
                    f"Expected day 1-7, got '{raw_day}'"
                )

            # Cron: day-of-week = specific day, day-of-month = "*"
            cron_day_of_week = self._parse_field(raw_day, 1, 7, "day of week")
            cron_day_of_month = "*"

        elif raw_month == "1" and raw_week == "0":
            # MONTHLY: month=1, week=0 means "every month"
            # Example: "0:1*0:15:12:0:0" = 15th of every month at 12:00
            # Day field specifies which date (1-31)
            if raw_day == "0" or raw_day == "":
                raise RecurToCronError(
                    f"Monthly pattern missing day: {recur_str}. "
                    f"Expected day 1-31, got '{raw_day}'"
                )

            # Cron: day-of-month = specific date, day-of-week = "*"
            cron_day_of_month = self._parse_field(raw_day, 1, 31, "day of month")
            cron_day_of_week = "*"

        elif raw_month == "1" and raw_week != "0":
            # ORDINAL: month=1, week>0 means "Nth weekday of month"
            # Example: "0:1*2:3:17:30:0" = 2nd Wednesday of every month
            # Cannot convert: cron doesn't support ordinal patterns
            raise RecurToCronError(f"Ordinal patterns not supported: {recur_str}.")

        else:
            # UNSUPPORTED: Any other month/week combination
            # Common cases: yearly (month>1), multi-week (week>1)
            raise ValueError(
                f"Unsupported pattern: {recur_str}. "
                f"month={raw_month}, week={raw_week} not recognized. "
                "Supported: daily(0,0), weekly(0,1), monthly(1,0)."
            )

        # Validate Year field (must be '0' or '*' for 'every year' in 6-field cron)
        if processed_year not in ["0", "*"]:
            raise ValueError(
                f"Specific year '{processed_year}' is not supported. Use '0' or "
                "'*' for 'every year'."
            )

        # Assemble the 6-field cron expression: SECOND MINUTE HOUR DAY-OF-MONTH MONTH DAY-OF-WEEK
        cron_expression = f"{cron_second} {cron_minute} {cron_hour} {cron_day_of_month} {cron_month} {cron_day_of_week}"

        return cron_expression

    @staticmethod
    def _parse_field(
        value_str: str, min_val: int, max_val: int, field_name: str
    ) -> str:
        """
        Parses a field value from Date::Manip::Recur format to a cron expression.

        Args:
            value_str: The field value to parse.
            min_val: The minimum allowed value for the field.
            max_val: The maximum allowed value for the field.
            field_name: The name of the field.

        Returns:
            A cron expression field value.

        Raises:
            ValueError: For invalid format, unsupported patterns, or conflicting Day/DayOfWeek.
        """
        if value_str == "*":
            return "*"

        # Reject any '*' not used as a standalone wildcard.
        if "*" in value_str:
            raise ValueError(
                f"Unsupported pattern in {field_name}: '{value_str}'. Only integers or '*' are allowed."
            )

        try:
            val = int(value_str)
            if not (min_val <= val <= max_val):
                raise ValueError(
                    f"Value '{value_str}' out of range for {field_name}. Expected {min_val}-{max_val}."
                )
            return str(val)
        except ValueError:
            raise ValueError(
                f"Invalid {field_name} format: '{value_str}'. Expected integer or '*'."
            )
