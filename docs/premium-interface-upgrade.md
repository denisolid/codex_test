# SkinAlpha Premium Interface Upgrade

## 1. Full UI/UX Audit (Current State)

### Global visual system
- Multiple parallel token groups (`--space-*` and `--ui-space-*`, several accent variables, repeated status palettes) create inconsistent spacing, emphasis, and tone.
- High chroma gradients, glows, and border effects are used simultaneously across nav, cards, rows, and modals, increasing visual competition.
- Warning and risk tones are overused for non-critical states, reducing signal clarity for true risk.
- Shared components (buttons, badges, cards, table rows, drawers, modals) have style drift by section.

### Information hierarchy
- Several screens place diagnostics and implementation-level status in the same visual layer as primary actions.
- Long helper text stacks create vertical noise and split attention before key actions.
- Duplicate state messaging appears in multiple places (status badges + helper text + row labels).

### Feed/list views
- Global opportunities table includes many strong visual cues at once (row cards, verdict, badges, hover effects, media effects, action buttons).
- Row-level emphasis can overshadow primary decision attributes (profit, quality, liquidity).
- Secondary notes and advanced diagnostics appear inline instead of being progressively disclosed.

### Insight/detail surfaces
- Compare/inspect/insight modals are data rich but visually dense due multi-tone badges and repeated contrast spikes.
- Section boundaries and typography hierarchy are not consistent enough for quick trust scanning.

### Navigation and controls
- Navigation chips and headers use multiple gradients and active styles that compete with content.
- Control labeling varies in style and granularity (technical vs user-oriented wording).

### Microcopy
- Several labels read like internal/system language instead of product outcomes.
- Some helper text is too long for high-frequency workflows.

## 2. Unified Premium Design System Proposal

### Core principles
- Calm dark surfaces with subtle depth.
- Single primary accent family for interaction.
- Strong hierarchy: primary action first, context second, diagnostics on demand.
- Dense information, lower visual noise.

### Color system
- Foundation: neutral deep navy surfaces.
- Accent: one blue interaction family.
- Semantic tones:
  - Positive: green.
  - Caution: amber.
  - Critical: reserved red.
- Remove multi-accent competition except where genuinely needed (rare category cues).

### Typography hierarchy
- Tighten heading scale and reduce uppercase overload.
- Primary numeric values remain strong; secondary metadata moves to muted tone.
- Helper text defaults to concise single-line intent where possible.

### Spacing system
- Unified scale: `4 / 8 / 12 / 16 / 24 / 32`.
- Standardized padding for panels, tables, and controls.
- Consistent vertical rhythm between toolbar, summary, table/list, and secondary details.

### Component standards
- Cards/Panels: one surface language, subtle borders, restrained shadows.
- Buttons: clear primary/secondary/tertiary hierarchy; minimal motion.
- Badges: restrained palette; one dominant verdict per entity.
- Inputs: consistent height, border radius, focus ring, background tone.
- Tables: calmer header, reduced row effects, clearer scan rows.
- Drawers/Modals: same shell and elevation model as core panels.

## 3. Prioritized Redesign Plan (Screen by Screen)

### P0 (implemented in this pass)
- Global tokens + base components (buttons, inputs, panels, tables, badges, modal/drawer shells).
- Navigation chrome and active states simplified.
- Global opportunities feed hierarchy rebuilt:
  - Toolbar actions kept.
  - New summary cards for feed/status/plan.
  - Diagnostics moved under progressive disclosure (`Scanner diagnostics`).
  - Cleaner row presentation and reduced visual effects.
- High-traffic microcopy cleanup (tabs, holdings workspace, inspector, exports, alerts, team, dashboard labels).

### P1
- Portfolio workspace:
  - Tighten card/table parity.
  - Reduce duplicate status labels between cards and row metadata.
  - Improve action grouping for inspect/compare/sync.
- Dashboard analytics:
  - Clarify KPI vs deep analytics layers.
  - Keep deep metrics collapsed by default with stronger section labeling.

### P2
- Alerts center:
  - Compact form hints and action labels.
  - Better distinction between configured alerts vs event history.
- Market tab and compare drawer:
  - Align metric cards and confidence language with feed standards.

### P3
- Settings/account and auth/onboarding:
  - Normalize panel rhythm and copy tone.
  - Reduce instructional verbosity while preserving guardrails.

## 4. Component Cleanup Plan

### Canonical primitives
- `Panel`/surface shell (single elevation + border system).
- `Button` variants (`primary`, `secondary`, `tertiary`, `ghost`) with unified sizing.
- `Badge` variants (`neutral`, `positive`, `warning`, `critical`) and a dedicated verdict style.
- `Form controls` (input/select/textarea) with shared focus treatment.
- `Table shell` (header, row hover, action cell rhythm).
- `Overlay shell` (drawer + modal).

### Cleanup actions
- Remove duplicate/high-variance color treatments in component-specific blocks.
- Migrate ad hoc gradients/glows to token-driven defaults.
- Keep category-specific coloration subtle and non-dominant.

## 5. Microcopy Cleanup Plan

### Rules
- Replace internal phrasing with user-outcome language.
- Keep helper text short and operational.
- Use one primary action verb per control.

### Applied now
- Tab hints simplified (`Holdings`, `Live opportunities`, `Global scanner`, etc.).
- Global scanner primary labels simplified (`Refresh feed`, `High-confidence only`, `Include older entries`, `Item type`).
- Portfolio/trades/alerts/team copy shortened and clarified.

### Next sweep
- Standardize all status strings across compare, insight, market, and account screens.
- Rename any remaining internal diagnostics labels that appear outside disclosure blocks.

## 6. Implementation Roadmap (No Functional Regression)

### Phase A: System foundation (done)
- Token refresh in shared CSS.
- Shared component visual normalization.
- Calm nav + modal/drawer + table baseline.

### Phase B: Feed and decision flow (done)
- Rebuilt opportunities info architecture with summary-first layout.
- Moved diagnostics into collapsible disclosure.
- Reduced row-level visual overload while preserving all actions/data.

### Phase C: Remaining surfaces
- Portfolio/dashboard/account/market copy + hierarchy harmonization.
- Component adoption audit to remove remaining one-off styles.

### Regression guardrails
- No endpoint or state model changes.
- No event handler or action wiring changes.
- Functional UI controls preserved (filters, pagination, compare, insight, refresh, alerts).
- Validate with frontend build + key interaction smoke checks.
