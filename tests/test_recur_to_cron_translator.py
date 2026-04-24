# (C) 2025 GoodData Corporation
import csv
import os

from gooddata_legacy2cloud.scheduled_exports.recur_to_cron.translator import (
    RecurToCronTranslator,
)

DATA_PATH = os.path.join(
    os.path.dirname(__file__),
    "data",
    "scheduled_exports",
    "recur_to_cron_test_data.csv",
)


def test_date_manip_to_cron_bulk() -> None:
    """
    Test the DateManipToCronTranslator against all test cases in data.csv.

    Calculates success ratio and provides detailed reporting for analysis.
    Goal: achieve ~80% success ratio while keeping the converter generic.

    Run pytest with -s to see the detailed output.
    """
    converter = RecurToCronTranslator()

    # Statistics tracking
    total_cases = 0
    successful_conversions = 0
    expected_raises = 0
    unexpected_raises = 0
    mismatches = []
    unexpected_successes = []

    with open(DATA_PATH, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row_num, row in enumerate(reader, start=1):
            total_cases += 1
            input_str = row["Email Recurrence"].strip()
            expected = row["Cron Expression"].strip()

            # Case 1: Expected to raise an error
            if expected == "raise":
                expected_raises += 1
                try:
                    actual = converter.convert_date_manip_to_cron(input_str)
                    # Converter succeeded when it should have failed
                    unexpected_successes.append(
                        {
                            "row": row_num,
                            "input": input_str,
                            "description": row["Email Frequency"],
                            "actual": actual,
                            "expected": "raise",
                        }
                    )
                except Exception:
                    # Expected behavior - converter raised an error
                    pass

            # Case 2: Expected to succeed
            else:
                try:
                    actual = converter.convert_date_manip_to_cron(input_str)
                    if actual == expected:
                        successful_conversions += 1
                    else:
                        # Converter succeeded but with wrong output
                        mismatches.append(
                            {
                                "row": row_num,
                                "input": input_str,
                                "description": row["Email Frequency"],
                                "expected": expected,
                                "actual": actual,
                            }
                        )
                except Exception as e:
                    # Converter failed when it should have succeeded
                    unexpected_raises += 1
                    mismatches.append(
                        {
                            "row": row_num,
                            "input": input_str,
                            "description": row["Email Frequency"],
                            "expected": expected,
                            "error": str(e),
                        }
                    )

    # Calculate success ratio
    success_ratio = (
        (successful_conversions / total_cases) * 100 if total_cases > 0 else 0
    )

    # Print comprehensive report
    print(f"\n{'=' * 60}")
    print("DATE::MANIP::RECUR TO CRON CONVERTER TEST RESULTS")
    print(f"{'=' * 60}")
    print(f"Total test cases: {total_cases}")
    print(f"Successful conversions: {successful_conversions}")
    print(f"Expected raises (handled correctly): {expected_raises}")
    print(f"Unexpected raises (should have succeeded): {unexpected_raises}")
    print(f"Unexpected successes (should have failed): {len(unexpected_successes)}")
    print(f"Output mismatches: {len(mismatches)}")
    print(f"Success ratio: {success_ratio:.1f}%")
    print(f"{'=' * 60}")

    # Report unexpected successes (converter succeeded when it should have failed)
    if unexpected_successes:
        print("\n❌ UNEXPECTED SUCCESSES (should have raised):")
        for case in unexpected_successes:
            print(f"  Row {case['row']}: {case['description']}")
            print(f"    Input: {case['input']}")
            print(f"    Output: {case['actual']}")
            print()

    # Report mismatches and unexpected raises
    if mismatches:
        print("\n❌ MISMATCHES AND ERRORS:")
        for case in mismatches:
            print(f"  Row {case['row']}: {case['description']}")
            print(f"    Input: {case['input']}")
            if "error" in case:
                print(f"    Expected: {case['expected']}")
                print(f"    Error: {case['error']}")
            else:
                print(f"    Expected: {case['expected']}")
                print(f"    Actual: {case['actual']}")
            print()

    # Summary and recommendations
    print("\n📊 ANALYSIS:")
    if success_ratio >= 80:
        print(
            f"✅ Excellent! Success ratio of {success_ratio:.1f}% meets the 80% target."
        )
    elif success_ratio >= 60:
        print(
            f"⚠️  Good progress! Success ratio of {success_ratio:.1f}% is close to target."
        )
    else:
        print(
            f"❌ Needs improvement. Success ratio of {success_ratio:.1f}% is below target."
        )

    if unexpected_raises > 0:
        print(
            f"🔧 Focus on fixing {unexpected_raises} cases where converter raises when it shouldn't."
        )

    if unexpected_successes:
        print(
            f"🔧 Focus on adding validation for {len(unexpected_successes)} cases where converter succeeds when it shouldn't."
        )

    # Comment/uncomment the line below to assert the success threshold
    assert success_ratio >= 80, (
        f"Success ratio {success_ratio:.1f}% is below 80% target"
    )
