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
