# DQ — HireStream

Data-quality framework and check catalog (SPEC §12). Filled in T2.11; COE-001 adds S-JB-03 (T5.5).

## Framework
<!-- Check declaration (config/dq/checks.yaml), check types, results tables, severity vs blocking. -->

## Circuit breaker
<!-- A failing blocking check stops gold publishing and the warehouse load; consumers keep the last
good data. -->

## Alerts
<!-- Console, Slack, SNS; payload fields; RUNBOOK anchors. -->

## Catalog
| ID | Layer | Table | Check | Severity | Blocking | RUNBOOK |
|---|---|---|---|---|---|---|

## Quarantine reason codes
| Code | Meaning | Source |
|---|---|---|
| `MALFORMED_JSON` | not JSON, duplicate keys, or not an object | both |
| `MISSING_REQUIRED_FIELD` | an envelope field or the entity key (`payload.interview_id`, `context.session_id`) is missing, null or can't be cast | both |
| `INVALID_ENUM` | event type, source, or an enum field outside its contract | both |
| `UNKNOWN_SCHEMA_VERSION` | no contract for (`event_type`, `schema_version`) | both |
| `UNPARSEABLE_TIMESTAMP` | a time not in its contract format; for the job board, `event_ts` below the contracts' minimum (epoch seconds) or past year 9999 | both |
| `UNRESOLVABLE_TIMEZONE` | a naive start with no zone, or one the JVM doesn't know | scheduling |
| `OUT_OF_RANGE_TIMESTAMP` | `event_ts` more than 1 h after its object arrived | both |

The first failing code in this order wins (ADR-0019 §3, ADR-0020). Fields outside the quarantine
set that are missing or can't be cast arrive as null; DQ owns them (a renamed `req_id` is COE-001).

Each run's `ground_truth.json` has `expected_quarantine`: the lines silver must quarantine, per
source and reason (ADR-0014 §5, ADR-0015).
