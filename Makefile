# (C) 2026 GoodData Corporation
.PHONY: dev
dev:
	uv sync
	uv run pre-commit install

	@echo "\n\nRun 'source .venv/bin/activate' to activate the virtual environment"

.PHONY: format
format:
	.venv/bin/ruff format .

.PHONY: format-check
format-check:
	.venv/bin/ruff format --check .

.PHONY: lint
lint:
	.venv/bin/ruff check

.PHONY: type
type:
	.venv/bin/ty check

.PHONY: test
test:
	.venv/bin/pytest


.PHONY: check
check: format lint test type
