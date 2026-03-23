# (C) 2026 GoodData Corporation
import argparse
import sys
from collections.abc import Callable

from gooddata_platform2cloud.workflows.generate_web_compare import (
    generate_web_compare_cli,
)
from gooddata_platform2cloud.workflows.migrate_color_palette import (
    migrate_color_palette_cli,
)
from gooddata_platform2cloud.workflows.migrate_dashboard_permissions import (
    migrate_dashboard_permissions_cli,
)
from gooddata_platform2cloud.workflows.migrate_dashboards import migrate_dashboards_cli
from gooddata_platform2cloud.workflows.migrate_insights import migrate_insights_cli
from gooddata_platform2cloud.workflows.migrate_ldm import migrate_ldm_cli
from gooddata_platform2cloud.workflows.migrate_metrics import migrate_metrics_cli
from gooddata_platform2cloud.workflows.migrate_pixel_perfect_dashboards import (
    migrate_pixel_perfect_dashboards_cli,
)
from gooddata_platform2cloud.workflows.migrate_reports import migrate_reports_cli
from gooddata_platform2cloud.workflows.migrate_scheduled_exports import (
    migrate_scheduled_exports_cli,
)

_COMMANDS: dict[str, Callable[[], None]] = {
    "ldm": migrate_ldm_cli,
    "metrics": migrate_metrics_cli,
    "insights": migrate_insights_cli,
    "dashboards": migrate_dashboards_cli,
    "reports": migrate_reports_cli,
    "color-palette": migrate_color_palette_cli,
    "scheduled-exports": migrate_scheduled_exports_cli,
    "pp-dashboards": migrate_pixel_perfect_dashboards_cli,
    "dashboard-permissions": migrate_dashboard_permissions_cli,
    "web-compare": generate_web_compare_cli,
}


def main() -> None:
    """Entry point for the gooddata-platform2cloud CLI."""
    parser = argparse.ArgumentParser(
        prog="gooddata-platform2cloud",
        description="GoodData Platform to Cloud metadata transfer tool.",
    )
    subparsers = parser.add_subparsers(
        dest="command",
        metavar="command",
        help="Available commands: " + ", ".join(_COMMANDS.keys()),
    )
    for name in _COMMANDS:
        subparsers.add_parser(name, add_help=False)

    args, _ = parser.parse_known_args(sys.argv[1:2])
    if not args.command:
        parser.print_help()
        sys.exit(1)
    sys.argv = [sys.argv[0]] + sys.argv[2:]
    _COMMANDS[args.command]()
