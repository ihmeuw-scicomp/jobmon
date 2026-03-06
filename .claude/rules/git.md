# Git Rules

## Branch Naming
- Features: `feature/<description>`
- Bug fixes: `fix/<description>`
- Releases: `release/<version>`

## Branch Workflow
- Feature work goes on `feature/` branches, never directly on `release/` branches
- Avoid `git stash` for complex branch operations; prefer WIP commits
- After upstream merge, diff ALL domain-related files for compatibility (not just conflict resolution)

## Versioning
This is a monorepo with separate versions per package:
- Tags: `client-X.Y.Z`, `core-X.Y.Z`, `server-X.Y.Z`
- Each sub-project has its own `pyproject.toml`

## Commit Messages
- Use present tense, descriptive messages
- Reference issue numbers when applicable

## Pre-Commit Hooks
- Hooks run formatters (prettier, black, isort) that modify files, causing commit to fail
- **Always format before committing:**
  - Backend: `uv run nox -s format`
  - Frontend: `cd jobmon_gui && npm run format`
- After hook failure: re-stage modified files, create NEW commit (never amend)
- With unstaged changes present, formatter hooks create stash/restore conflicts - stage ALL changes first

## CHANGELOG.md
- Maintained at repo root in Keep a Changelog format (`[Unreleased]`, `### Added/Changed/Fixed/Removed`)
- If missing from current branch, restore from latest release branch (`release/3.x`)
- Cover ALL commits since last release, not just current feature branch
