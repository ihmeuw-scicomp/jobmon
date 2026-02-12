# Frontend Rules

## Component Development

- Audit existing components before creating new UI elements
- Check `jobmon_gui/src/components/` for reusable components
- Avoid duplicating functionality that already exists
- When applying a UI pattern change, apply to ALL instances across ALL panels
- Check for redundant displays when adding new info sections
- When modifying a component with multiple view states (selected/unselected, loading/error/empty), address ALL states

## React Patterns

- Hooks must be placed BEFORE any conditional `return` statements (Rules of Hooks). `npm run build` does NOT catch this - it's a runtime-only error
- When adding hooks like `useTheme()` or `useMediaQuery()`, scan for early returns first
- Use individual MUI imports (`import Box from '@mui/material/Box'`), not barrel imports
- react-router `<Link>` needs explicit styling in MUI context (`color`, `textDecoration`)

## TypeScript Patterns

- Path aliases: Use `@jobmon_gui/` prefix in imports
- Type regeneration: Run `uv run nox -s generate_api_types` after API changes

## Generated Files

- NEVER manually edit `jobmon_gui/src/types/apiSchema.ts` - it is auto-generated
- After backend Pydantic schema changes: restart Docker backend, then regenerate types:
  ```bash
  docker compose restart jobmon_backend && sleep 5 && uv run nox -s generate_api_types
  ```

## Plotly.js

- Use `plotly.js-dist` (pre-built) with `react-plotly.js/factory` pattern. Never import `plotly.js` directly
- Reference pattern: `TaskConcurrencyTab.tsx`
- Suppress `onDeselect` when external selection is active to prevent re-render loops

## ReactFlow

- Separate layout computation (dagre) from visual state (hover, selection)
- Use `useSyncExternalStore` for visual state; keep node/edge arrays referentially stable
- `fitView` should depend on structural key (sorted node IDs), not full node array
- Never use `animated: true` on edges (causes CSS animation restart jank)

## UI Conventions

- Default to multi-select for filter controls (dropdowns, selectors)
- Maintain consistent color schemes across related components
- Follow existing patterns in similar components
- Every `IconButton` must be wrapped in a MUI `<Tooltip>` with descriptive title
- Action controls go immediately below the header, not at bottom of panels
- Use compact spacing for dashboard layouts: `spacing={1}`, `p: 1-2`
- Chart filters: inline horizontal bars (always visible), not hidden in popovers
- Dropdowns for actions need verb-based labels (e.g., "Set Status" not "All Tasks")
- CSS flex sidebars: pair `flex: '0 0 Xpx'` with `maxWidth: X` and `minWidth: 0`

## Status Model

- Status colors live in `constants/taskStatus.ts` - import from there, never define inline
- ERROR (retriable, `#d55e00`) and FATAL (permanent, `#cc3311`) are distinct statuses - never merge them
- Backend key `LAUNCHED` displays as `Scheduled` in UI
