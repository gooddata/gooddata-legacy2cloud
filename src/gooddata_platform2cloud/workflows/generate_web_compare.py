# (C) 2026 GoodData Corporation
"""
Script to generate web comparison HTML from migration log files.
"""

import logging
from time import time

from gooddata_platform2cloud.arg_parsing.arg_parser import parse_web_compare_cli_args
from gooddata_platform2cloud.config.configuration_objects import WebCompareConfig
from gooddata_platform2cloud.config.env_vars import EnvVars
from gooddata_platform2cloud.helpers import duration
from gooddata_platform2cloud.logging.config import configure_logger
from gooddata_platform2cloud.web_compare_processing import LogProcessor
from gooddata_platform2cloud.web_compare_processing.cli_processor import (
    process_log_directory,
)

logger = logging.getLogger("migration")
configure_logger()


def generate_web_compare(config: WebCompareConfig):
    """Generate web comparison HTML from migration log files."""
    start_time = time()

    # Load environment variables (only used as fallback if not in log files)
    env_vars = EnvVars(config.env)
    env_vars.log_connection_info()

    processor = LogProcessor(env_vars, config.output_dir, not config.skip_inherited)
    output = process_log_directory(config, processor)

    execution_time = duration(start_time)
    logger.info("----DONE in %.2fs----", execution_time)

    return output


def generate_web_compare_cli():
    args = parse_web_compare_cli_args()
    config = WebCompareConfig.from_kwargs(**args.__dict__)
    generate_web_compare(config)
