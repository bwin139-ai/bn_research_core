# AGENTS.md

This file is the root operating guide for AI agents working in this repository.
It exists so a new Codex thread can recover the project rules from the repo
instead of relying on chat memory.

## 1. Startup Checklist

At the start of every new thread:

1. Run `git status --short`.
2. Read `docs/README.md`.
3. Read the active baseline documents:
   - `docs/PROJECT_BASELINE.md`
   - `docs/STANDARD_PATCH_FRAMEWORK.md`
   - `docs/CURRENT_STATE.md`
4. If the task touches a specific strategy, read that strategy's semantic
   baseline, for example:
   - `docs/Spring-SABC项目语义基线.md`
5. Inspect code, logs, state files, and run outputs before making conclusions.

Do not use old chat memory as project truth. Repository files and runtime facts
are the truth source.

## 2. Document Priority

When documents conflict, use this priority:

1. `docs/PROJECT_BASELINE.md`
2. `docs/STANDARD_PATCH_FRAMEWORK.md`
3. `docs/CURRENT_STATE.md`
4. Strategy semantic baseline documents
5. Code-flow notes, reports, and archived material

Files under `docs/archive/` are historical references only. They are not active
baseline documents.

## 3. Non-Negotiable Project Rules

- LONG-only: do not introduce SHORT semantics, fields, branches, tests, or
  implementation paths.
- Fact-first: code, logs, state files, persisted outputs, and command results
  are facts. Missing facts must be gathered before conclusions.
- Fail fast: missing config, missing fields, semantic conflicts, bad data, and
  inconsistent state must stop the flow instead of being hidden by fallback.
- No silent defaults: do not add default values, compatibility shims, or legacy
  field aliases to mask errors.
- One semantic meaning maps to one field and one implementation path.
- `sim` is the strategy semantic baseline; `live` must be audited against it.
- Do not push to remote or touch production deployment without explicit user
  approval.

## 4. Patch Discipline

For any persistent source-code change:

1. Identify the single main issue.
2. Classify the patch as exactly one of:
   - `PERF_ONLY`
   - `ARCH_ONLY`
   - `LOGIC_ONLY`
3. Lock the target file baseline before editing.
4. Make the smallest necessary change.
5. Verify with the smallest meaningful command or run.
6. Report changed files, verification, and remaining risks.

Do not mix unrelated cleanup, refactors, naming changes, or formatting churn into
a patch.

## 5. Working With Local Changes

The worktree may contain user changes. Never revert or overwrite changes you did
not make unless the user explicitly asks. If user changes affect the current
task, work with them and call out the interaction.

## 6. Documentation Maintenance

When a task changes project state, update `docs/CURRENT_STATE.md`.
When a document becomes stale but still has historical value, move it under
`docs/archive/` instead of keeping it in the active docs root.
Do not store secrets, API tokens, or one-off production commands in active docs.
