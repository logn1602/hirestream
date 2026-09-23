---
description: Finish the current task — verify, document, push, open the PR
---
1. Run `make lint test`, plus `make e2e` if pipeline code changed. Fix failures; never skip hooks.
2. Update every doc this task affects (DESIGN, DATA_MODEL, METRICS, DQ, TUNING, COST, RUNBOOK, NOTES, TALKING_POINTS, ADRs) and docs/PROGRESS.md: tick the task and update the "Current phase / Next task / Last updated" line.
3. Review `git status` and `git diff --stat`, then commit what's left with Conventional Commit messages and a `Refs: <task id>` footer.
4. `git push -u origin HEAD`, then `gh pr create` with a Conventional-Commit-style title and a body that follows .github/pull_request_template.md.
5. Add a row to the PROGRESS.md session log (date, task, PR link, one-line outcome); commit it as `docs(progress): log <task id>` and push.
6. `gh pr checks --watch`. If CI fails, fix, commit, push, and watch again.
7. Report: PR link, what changed and why, how it was tested, what I should review closely, and **What to understand** (key concept, the alternative you rejected and why, one likely interview question with a strong answer).
8. Don't merge. Wait for me to say "merge".
