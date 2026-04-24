# GoodData Python Coding Standards

## File Headers

First line of every new file:
```python
# (C) 2025 GoodData Corporation
```

## Python Version & Style

- Target: Python 3.14
- PEP 8, 4-space indent, 80-char line limit
- Naming: `snake_case` vars/functions, `PascalCase` classes, `UPPER_SNAKE_CASE` constants

## Type Hints

All new code requires comprehensive type hints (params, return values, class attributes).
- Use built-in generics: `list[str]`, `dict[str, Any]`, `tuple[int, str]`
- Use modern syntax (`Type | None` not `Optional[Type]`)
- ty is enabled for type-checking; no `# type: ignore` — fix the underlying issue

## Data Structures

- Pydantic `BaseModel` for validated data
- `attrs @define` for simple data classes without validation
- Raw dicts acceptable for legacy code — don't refactor existing code

## Code Organization

- Single responsibility per function/class/module
- Prefer composition over inheritance
- All imports at top of file, PEP 8 order (stdlib → third-party → local)

## Validation

Run `make check` before completing any code change. Runs: format, lint, tests, type-check.
Individual targets: `make format`, `make lint`, `make test`, `make type`.
