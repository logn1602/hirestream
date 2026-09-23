---
description: Start the next HireStream task from docs/PROGRESS.md
argument-hint: "[optional task id, e.g. T1.3]"
---
Start task $ARGUMENTS. If no task id was given, take the first unchecked task in docs/PROGRESS.md.

1. Verify identity: `git var GIT_AUTHOR_IDENT` must show Shubh Dave with his GitHub email, and `gh auth status` must show logn1602. If either is wrong, stop and tell me.
2. `git switch main && git pull --ff-only` (skip the pull if no remote exists yet). The working tree must be clean.
3. Read the task in docs/PROGRESS.md, every docs/SPEC.md section it references, related ADRs, and the code it will touch.
4. Reply with a plan: goal, files to create or change, tests, commands you'll run, risks and open questions, and the branch name (`<type>/<task-id>-<slug>`).
5. If the task touches infra or cloud, adds a dependency, or will change more than ~300 lines, stop and wait for my approval. Otherwise create the branch and start, committing in small Conventional Commits with tests alongside the code.
