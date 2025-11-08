# 0_top_fixes

| # | Title | Rationale | Scope | Impact | Risk |
|---|-------|-----------|-------|--------|------|
| 1 | Fail fast on enqueue insert errors | `enqueueCmd` suppresses `db.Exec` failures, so missing tables or constraint violations silently drop work items and report success. Propagate the error and abort so operators know the queue is unhealthy.【F:main.go†L126-L135】 | `enqueueCmd` | Correctness | Med |
| 2 | Enforce schema migrations | `openDB` ignores failures when applying `.quality/schema.sql`, leaving the database without required tables while subsequent commands proceed until they later fail mysteriously. Surface the error immediately.【F:main.go†L79-L93】 | `openDB` | Correctness | Med |
| 3 | Handle status row scan errors | `statusCmd` discards `rows.Scan` errors, which can print garbage or crash later if columns change. Check the error per row and after iteration to keep diagnostics accurate.【F:main.go†L252-L258】 | `statusCmd` | Correctness | Low |

# 1_summary_verdict
- Grade: Needs Refactoring
- Score: 2
- Justification: Core queue operations run, but unchecked database errors and loose result handling leave the CLI silently misbehaving under schema issues or data corruption.

# 2_file_naming
- Score: 3
- Assessment: `main.go` houses the CLI entrypoint plus all subcommands; acceptable but edging toward a catch-all.
- Rename suggestion: Consider `cmd/next/main.go` with subcommand files if the binary grows.
- Evidence: Multiple top-level command handlers coexist in one file.【F:main.go†L20-L297】

# 3_cohesion_srp
- Score: 2
- Assessment: File mixes CLI wiring, database access helpers, hashing utilities, and every subcommand, which strains single-responsibility as the tool expands.
- split_if: Extract DB helpers and individual command logic once features grow further.
- Recommendations: `internal/db/db.go ← move: openDB`; `cmd/next/enqueue.go ← move: enqueueCmd`; `cmd/next/status.go ← move: statusCmd`.
- Evidence: Helper utilities and each command share the same file without modular boundaries.【F:main.go†L60-L297】

# 4_identifier_naming
- Score: 4

| Identifier | Exported? | Issue | Proposed | Reason |
|------------|-----------|-------|----------|--------|
| defaultDB | no | vague | defaultDBPath | clearer |

- Notes: Rest of the identifiers are concise and domain-aligned.【F:main.go†L18】

# 5_function_excellence
- Score: 2

`main` (lines 20-40)
- readability: Straightforward switch over subcommands; concise control flow.【F:main.go†L20-L40】
- error_handling: Relies on individual command handlers for diagnostics; acceptable.
- efficiency: O(1); no concerns.
- refactor: None needed at current size.

`usage` (lines 43-58)
- readability: Large raw string literal is readable but could trim trailing newline.【F:main.go†L43-L57】
- error_handling: n/a.
- efficiency: n/a.
- refactor: Consider storing help text in const if reused.

`pathHash` / `fileHash` (lines 61-77)
- readability: Clear utilities.【F:main.go†L61-L77】
- error_handling: `fileHash` propagates open/copy errors; good. Closing errors ignored (usually fine).
- efficiency: Uses streaming hashing; good.
- refactor: none.

`openDB` (lines 79-94)
- readability: Compact helper.【F:main.go†L79-L93】
- error_handling: Ignores schema application errors and potential `db.Close` failure. Return wrapped errors to expose migration issues.
- efficiency: Acceptable for CLI usage.
- refactor: Capture and return `db.Exec(string(schema))` errors; optionally reuse prepared statements.

`enqueueCmd` (lines 96-136)
- readability: Loop is clear though silent `continue` on errors hides context.【F:main.go†L109-L135】
- error_handling: Fails to report `scanner.Err()`, file hash errors, and especially insert failures, so operators receive a misleading success count. Log skipped paths and exit on insert error.【F:main.go†L122-L134】
- efficiency: Hashing each file is expected cost; consider using larger scanner buffer for long paths.
- refactor: After loop, check `scanner.Err()` and fail accordingly; log reasons for skips to stderr.

`claimCmd` (lines 138-172)
- readability: Straightforward query.【F:main.go†L153-L171】
- error_handling: Continues silently on `rows.Scan` errors and never checks `rows.Err()`. Return fatal error to avoid partial claims.【F:main.go†L165-L170】
- efficiency: Query uses indexable filters; fine.
- refactor: Validate `n > 0` early; include ordering by `next_at` if relevant.

`doneCmd` (lines 174-217)
- readability: Clear parameter parsing and update.【F:main.go†L174-L216】
- error_handling: Good flag validation; consider surfacing revisit parsing issues (currently trusts SQLite modifier blindly). Ensure DATETIME call only when revisit provided to avoid `DATETIME('now', NULL)` semantics confusion.【F:main.go†L201-L213】
- efficiency: Single UPDATE; fine.
- refactor: Wrap errors with context (e.g., include path).

`statusCmd` (lines 219-259)
- readability: Dynamic SQL assembly manageable.【F:main.go†L232-L258】
- error_handling: Ignores `rows.Scan` errors and `rows.Err()`, so display might contain stale data. Handle per-row errors and check after loop.【F:main.go†L252-L258】
- efficiency: Aggregation query fine for small data sets.
- refactor: Consider using `fmt.Fprintf(os.Stdout, ...)` for clarity.

`resetCmd` (lines 261-297)
- readability: Clear confirmation flow.【F:main.go†L268-L296】
- error_handling: Good validation; ignore `RowsAffected` error though low risk. Confirm prompts may block pipelines.
- efficiency: Single DELETE; fine.
- refactor: Add `rows.Err()` check though none expected.

# 6_comments_docs
- Score: 2
- godoc_coverage: No exported symbols; doc comments optional.
- quality: Only `// Hash utilities` comment adds little value; consider removing or expanding to explain deterministic hashing rationale.【F:main.go†L60】
- examples: Usage in `usage` string suffices.

# 7_go_specific_checklist
- error_wrapping: ✗ No context wrapping on DB errors; consider `fmt.Errorf("insert %s: %w", path, err)` in commands.【F:main.go†L126-L135】【F:main.go†L208-L215】
- errors_is_as: ✓ Not applicable.
- context: ✓ No context parameters expected for CLI.
- receivers: ✓ Functions are package-level; no receivers.
- zero_values: ✓ Makes good use of zero value pointers for optional args.【F:main.go†L201-L206】
- mutex_copy: ✓ None present.
- defer_loops: ✓ No defers in loops.
- time_after: ✓ Not used.
- loop_vars: ✓ No goroutines.
- prealloc: ✗ Could preallocate args slice with capacity 1 when treatment filter used.【F:main.go†L238-L243】
- panics: ✓ None.
- logging: ✗ Mixed stdout/stderr messaging without context structuring.【F:main.go†L135】【F:main.go†L214-L215】

# 8_metrics_thresholds
- functions_gt_60_lines: None.
- nesting_gt_3: None.
- switch_gt_8_cases: None.
- large_composites: None.

# 9_public_surface_risk
- exported_api: None.
- unclear_contracts: Not applicable.
- recommendations: n/a.

# 10_appendix_evidence
- L79-L93: `openDB` ignores schema execution errors.
- L126-L135: `enqueueCmd` drops insert failures silently.
- L165-L170: `claimCmd` suppresses row scan errors.
- L252-L258: `statusCmd` ignores scan errors and `rows.Err()`.
- L268-L296: `resetCmd` prompt/confirmation flow.
