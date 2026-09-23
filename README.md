# HireStream

A data platform for the recruiting and internal-mobility systems of **Halcyon**, a fictional
25,000-person company: multi-source event ingestion, a bronze/silver/gold lake, a Kimball
warehouse, data-quality gates, WBR-style metrics, query tuning, and an AWS deployment via CDK.

> **Status:** Phase 0 — bootstrapping. See [`docs/PROGRESS.md`](docs/PROGRESS.md).

- Design and requirements: [`docs/SPEC.md`](docs/SPEC.md)
- Simulation parameters: [`config/generator/base.yaml`](config/generator/base.yaml)

**Synthetic data — no real people.** Every person, company, and event is generated. No
demographic or protected attributes exist anywhere in the project, by design.

## License
[MIT](LICENSE)
