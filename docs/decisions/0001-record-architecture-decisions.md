# ADR-0001: Record architecture decisions

- **Status:** Accepted
- **Date:** 2026-09-23
- **Task:** T0.4

## Context
HireStream makes decisions that are not obvious from the code: which EMR release to use, how
local and cloud differ, and why one tool was picked over another. The spec (`docs/SPEC.md`) holds
defaults, and some of those defaults will turn out wrong or out of date. `CLAUDE.md` says to stop
and write an ADR instead of drifting from the spec without a trace. Every decision also has to be
defensible in an interview, so the reasoning must survive after the conversation that produced it.

## Decision
Record each significant decision as an Architecture Decision Record in `docs/decisions/`.

- **File name:** `NNNN-kebab-case-title.md`. Numbers are four digits, sequential, and never reused.
- **Sections:** Status, Date, Task, then Context, Decision, Alternatives considered, Consequences.
  Add a References section when the decision rests on external docs.
- **Status values:** Proposed → Accepted → (optionally) Superseded by ADR-NNNN or Deprecated.
- **Immutability:** once an ADR is merged, only its Status line changes. To change a decision, write a
  new ADR that supersedes the old one, and link the two.
- **When an ADR is required:**
  - a deviation from `docs/SPEC.md`
  - any change to a probability, distribution, or calibration target in `config/generator/base.yaml`
  - a runtime or platform version pin
  - adding or removing a major dependency or AWS service
  - a known gap between local and cloud behaviour
- **Where it lands:** in the same PR as the change it justifies. The spec is updated to point at the ADR.

`docs/decisions/TEMPLATE.md` (T0.5) copies this structure.

## Alternatives considered
- **Decisions only in PR descriptions.** Rejected. They are hard to find later, they are tied to
  GitHub, and they get split across many PRs.
- **Editing `docs/SPEC.md` in place.** Rejected. The spec shows the current state, not why it changed
  or what else was considered. Changes to the spec still happen, but they link to an ADR.
- **A single `DECISIONS.md` log.** Rejected. One file per decision gives each decision its own review
  diff, a stable link, and a status that can be superseded.

## Consequences
- A small amount of extra writing for each significant decision.
- `docs/DESIGN.md` (T6.2) can summarise and link to ADRs instead of restating them.
- ADRs double as interview material: each one already states the alternative and why it lost.
