---
description: Merge the approved PR for the current branch and sync main
---
Run this only after I've said "merge" in this conversation.

1. `gh pr view --json number,title,state,mergeable,statusCheckRollup` — confirm the PR is open, mergeable, and every check passed. If not, stop and report.
2. `gh pr merge --merge --delete-branch`
3. `git switch main && git pull --ff-only`
4. `gh run list --branch main --limit 1` — confirm CI started on main, and report the merge commit SHA.
5. If this PR finished a phase, remind me about the milestone tag listed in docs/PROGRESS.md. Don't create tags unless I say so.
