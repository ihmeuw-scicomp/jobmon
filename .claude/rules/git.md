# Git Rules

## Branch Naming
- Features: `feature/<description>`
- Bug fixes: `fix/<description>`
- Releases: `release/<version>`

## Versioning
This is a monorepo with separate versions per package:
- Tags: `client-X.Y.Z`, `core-X.Y.Z`, `server-X.Y.Z`
- Each sub-project has its own `pyproject.toml`

## Commit Messages
- Use present tense, descriptive messages
- Reference issue numbers when applicable
