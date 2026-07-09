# Contributing

This document covers setting up your environment, repository conventions,
testing standards and the release flow.

## Development Setup

Requires Python 3.14+.

```bash
make dev
```

This runs `uv sync --all-groups` and installs the pre-commit hooks. Then:

```bash
source .venv/bin/activate
```

## Setting Up Your AI Assistant

If you're using an AI coding assistant, this repo already ships rules for
it:

- **Claude Code** picks up `.claude/rules/*.md` automatically
- **Cursor** picks up `.cursor/rules/*.mdc` automatically
- **Anything else** (Codex, etc.) — `AGENTS.md` points you at
  `.claude/rules/*.md`; copy the relevant files into your tool's config

The rule content covers architecture, coding standards, testing, and git
policy in more detail than this file — it's the same information kept in
sync with what's below. Note it's duplicated across `.claude/rules/` and
`.cursor/rules/`, so if you change one, change the other.

## Coding Standards

- File header on every new file: `# (C) {year} GoodData Corporation`
- Use hints on all new code
- Use Pydantic for anything that needs validation (API responses etc.), attrs
  for plain data objects.

Run `make check` before you consider a change done — it runs format, lint,
test, and type-check. Individually: `make format`, `make lint`, `make test`,
`make type`.

## Testing

Stack is pytest + pytest-mock (the `mocker` fixture). Test data is real,
sanitized Legacy/Cloud payloads under `tests/data/`, not synthetic fixtures.

Current test coverage is built around real data snapshots (sanitized Cloud
and Legacy responses) comparing the inputs for transformation and the expected
results. Unit tests exist for some modules but coverage could be improved.

Fixture placement: broadly-used fixtures go in `tests/conftest.py`,
domain-specific ones in `tests/fixtures/`. All new code needs unit tests,
and all external calls (API, file I/O) must be mocked — tests shouldn't hit
real APIs.

## Documentation

Any new migration flow, CLI argument, or output file needs to be documented
in the README, following its existing structure (update the Table of Contents
too).

Any significant changes to the CLI should be reflected in GoodData MCP
Server knowledge base as well.

## Release

Releases are manual — nothing ships automatically on merge to `master`.

A maintainer triggers the **"Bump version & trigger release"** workflow from
the Actions tab, picking a semantic version bump type (`major`, `minor`, or `patch`):

1. Bumps the version
2. Commits and tags it (`v{version}`) on a release branch, merges that
   branch into `master`
3. Creates a GitHub Release
4. Builds and publishes the package to PyPI.

`release-pypi.yaml` can also be run on its own against an existing tag, in
case a publish needs to be redone.

PyPI publishing is handled using Trusted Publisher settings.
