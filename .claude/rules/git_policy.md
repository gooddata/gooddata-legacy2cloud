# Git Policy

## Commit Messages

- Subject line: max 70 characters
- Required footer: `risk: <value>` — allowed values: `nonprod`, `low`, `high`

## Git Workflow

- Prefer `git push --force-with-lease` over `--force` for unpublished branches
- Autosquash/rebase only for unpublished (not yet shared) commits
