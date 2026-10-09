# HireStream

A data platform for the recruiting and internal-mobility systems of **Halcyon**, a fictional
25,000-person company: multi-source event ingestion, a bronze/silver/gold lake, a Kimball
warehouse, data-quality gates, WBR-style metrics, query tuning, and an AWS deployment via CDK.

> **Status:** Phase 1 complete: the generator. Phase 2, the batch pipeline, is next. See
> [`docs/PROGRESS.md`](docs/PROGRESS.md).

- Design and requirements: [`docs/SPEC.md`](docs/SPEC.md)
- Simulation parameters: [`config/generator/base.yaml`](config/generator/base.yaml)

## The generator at full scale
`hirestream generate backfill --preset full` simulates 18 months of a 25,000-person company. These
are the actuals SPEC §6.11 asks for, from seed 1602:

| | Measured | Target |
|---|--:|---|
| Stream events | 22.9 M (job board 22.2 M, scheduling 0.77 M) | ≥ 10 M |
| Bronze | 23.3 M lines in 26,314 hourly parts, plus 545 HRIS files (14.3 M rows): 2.0 GB | |
| ATS | 546 k applications, 10.3 k offers, 7.5 k hires; loaded into Postgres in 51 s (2.39 M rows, 363 MB) | |
| Runtime | 14 min by the run's clock (824 s, the ATS load included) on a quiet machine; 24 min on a busy one | ≤ 45 min |
| Peak memory | 1.3 GiB (Postgres: 274 MiB during the load) | ≤ 4 GB |
| Calibration | 18 of 18 targets within range | all |
| Contracts | every scheduling line, and 1 job-board line in 50, valid or failing exactly as injected | sampled at full |

- **Machine:** WSL2 Ubuntu with 8 vCPUs and a 3 GB VM, on a Windows laptop.
- **Runtime varies.** The run is CPU-bound, but the VM doesn't always get the CPU.
  - **Quiet machine:** 824 s by its own clock, the ATS load included.
  - **Busy machine:** 1,417 s, and 2,535 s with three other jobs sharing it. That last run took
    12 hours wall, because the laptop slept.
  - **Same output:** all three runs wrote byte-identical files.
  - **Details:** [ADR-0017](docs/decisions/0017-full-preset-acceptance.md).
- **Dev for comparison:** 1.5–1.8 M events in about 75 s at under 200 MiB, and 17 of 18 targets on
  each of five seeds. HT4 at dev is an explained miss
  ([ADR-0016](docs/decisions/0016-calibration-tuning-at-dev.md)).

**Synthetic data — no real people.** Every person, company, and event is generated. No
demographic or protected attributes exist anywhere in the project, by design.

## License
[MIT](LICENSE)
