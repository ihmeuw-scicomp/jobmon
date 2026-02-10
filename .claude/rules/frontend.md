# Frontend Rules

## Component Development

- Audit existing components before creating new UI elements
- Check `jobmon_gui/src/components/` for reusable components
- Avoid duplicating functionality that already exists

## TypeScript Patterns

- Path aliases: Use `@jobmon_gui/` prefix in imports
- Type regeneration: Run `uv run nox -s generate_api_types` after API changes

## UI Conventions

- Default to multi-select for filter controls (dropdowns, selectors)
- Maintain consistent color schemes across related components
- Follow existing patterns in similar components
