# DATA_MODEL — HireStream

Gold Kimball model: grains, keys, SCD rules (SPEC §10). Filled in T2.6–T2.10.

## Conventions
<!-- Surrogate keys (xxhash64), unknown member -1, date keys, UTC, precomputed durations. -->

## ERD
```mermaid
erDiagram
  %% T2.6–T2.10
```

## Dimensions
| Table | Type | Grain / key | Notes |
|---|---|---|---|
| `dim_date` | static | `date_key` | T2.6 |
| `dim_employee` | SCD2 | `employee_sk` | T2.7 |
| `dim_interviewer` | role-playing view | `employee_sk` | T2.9 |
| `dim_requisition` | SCD1 | `requisition_sk` | T2.6 |
| `dim_candidate` | SCD1 | `candidate_sk` | T2.6 |
| `dim_source_channel` | static | `channel_key` | T2.6 |

## Facts
| Table | Type | Grain | Task |
|---|---|---|---|
| `fct_application_pipeline` | accumulating snapshot | one row per `application_id` | T2.8 |
| `fct_interview` | transaction | (`interview_id`, interviewer) | T2.9 |
| `fct_jobboard_event` | transaction | one row per deduped event | T2.10 |
| `agg_jobboard_req_daily` | aggregate | req × day | T2.10 |
| `fct_employee_movement` | transaction | one row per version transition | T2.7 |

## SCD2 algorithm (`dim_employee`)
<!-- Snapshot comparison, record_hash, effective ranges, late and missing snapshots (T2.7). -->

## Reprocessing window
<!-- Touched partitions ∪ trailing 7 days; key re-resolution (SPEC §10.5). -->
