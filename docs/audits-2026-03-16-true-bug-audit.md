# Go Codebase True-Bug Audit (3 Pass)

## PASS 1 — Correctness & Reliability

### System Map
- **Entrypoint:** `main()` dispatches CLI subcommands `enqueue|claim|done|status|reset`.  
  Reachability (`snipe show 1be83b0a9e8c4ec2`): `main -> claimCmd|enqueueCmd|doneCmd|statusCmd|resetCmd`.
- **Persistence:** single SQLite DB opened via `openDB()` with WAL + schema/migration execution.
- **External boundaries:** filesystem hashing (`fileHash`, `dirHash`) and stdin ingestion (`enqueueFromStdin`).
- **Error model:** hard-fail via `fatal()` in commands; some warnings during batch scans.

### Findings (ranked)

1) **Sharding excludes a real tail of SHA256 keyspace**  
**Severity:** High
- **Evidence:** `calculateShardRange()` sets last shard upper bound to `ffffffffffffffff` then pads with 48 zeros; `buildClaimQuery()` applies `path_hash < shardEnd` for all shards including last. This excludes hashes in `(ffffffffffffffff000.., fffff...ffff]`. (`snipe pack calculateShardRange`, `snipe pack buildClaimQuery`).
- **Mechanism:** last shard still uses exclusive upper bound that is not max 256-bit value.
- **Scenario:** some jobs are never claimable on sharded workers, causing permanent starvation.
- **Minimal fix:** for last shard, omit upper bound (`AND path_hash >= ?` only) or set upper to 64 `f` and use `<=`.
- **Tests:** add property test that union of shard predicates covers `000..0` and `fff..f`.
- **Confidence:** High.

2) **`claim --n` accepts non-positive values, enabling accidental unbounded claims**  
**Severity:** High
- **Evidence:** `claimCmd()` parses `--n` but never validates it; `buildClaimQuery()` appends `LIMIT ?`. SQLite treats `LIMIT -1` as no upper limit. (`snipe pack claimCmd`, `snipe pack buildClaimQuery`).
- **Mechanism:** negative `n` bypasses batching guarantees.
- **Scenario:** one misconfigured worker claims the full queue, causing load spikes and unfairness.
- **Minimal fix:** reject `n <= 0` in `claimCmd()`.
- **Tests:** CLI test for `--n=0` and `--n=-1` expecting fatal.
- **Confidence:** High.

3) **`done --revisit` lacks validation and can silently disable revisit scheduling**  
**Severity:** Medium
- **Evidence:** `doneCmd()` passes raw `revisit` into `markDone()`; SQL uses `DATETIME('now', ?)` without validating modifier.
- **Mechanism:** invalid modifiers return `NULL`, but command exits success.
- **Scenario:** operators think item will requeue; it never does.
- **Minimal fix:** add `validRevisit()` (parallel to `validLease`) and reject invalid inputs.
- **Tests:** ensure invalid revisit string returns error and does not update row.
- **Confidence:** High.

4) **`enqueue` can abort entire transaction on long stdin lines (scanner token limit)**  
**Severity:** Medium (Plausible)
- **Evidence:** `enqueueFromStdin()` uses default `bufio.Scanner` (64 KiB token cap) and rolls back tx on `scanner.Err()`.
- **Mechanism:** one oversized line causes rollback of all prior successful rows.
- **Scenario:** generated path manifests or malformed stdin line triggers total enqueue failure.
- **Minimal fix:** set larger scanner buffer or switch to `bufio.Reader` line reads.
- **Tests:** feed >64KiB line and verify robust behavior.
- **Confidence:** Medium.

## PASS 2 — Concurrency & Lifecycle

### Concurrency Roots Inventory
- No explicit goroutine starts in production code (`go` statements absent in `main.go`).
- Concurrency is primarily **multi-process** via SQLite lease/claim semantics in `claimCmd -> buildClaimQuery`.

### Findings

1) **Lease ownership is not enforced when marking done (cross-worker overwrite)**  
**Severity:** High
- **Evidence:** `buildClaimQuery()` records `claimed_by`; `markDone()` updates by `(path_hash, treatment)` only and unconditionally clears claim fields. (`snipe pack buildClaimQuery`, `snipe pack markDone`).
- **Mechanism:** any worker/process can complete another worker’s claimed task.
- **Timeline scenario:** Worker A claims; Worker B (stale/buggy) calls done on same path and overwrites result, clearing A's lease.
- **Minimal fix:** require worker identity on done and predicate `AND claimed_by=?` (or compare-and-swap token).
- **Test strategy:** two-worker integration test asserting non-owner done is rejected.
- **Confidence:** High.

2) **No cancellation/deadline contexts for long DB ops and hashing loops**  
**Severity:** Medium (Plausible)
- **Evidence:** command paths use `context.Background()` throughout; no `WithTimeout` or signal cancellation wiring.
- **Mechanism:** under lock contention or slow IO, process cannot gracefully time out internal operations.
- **Timeline scenario:** DB lock + busy environment causes long hangs/retries until external kill.
- **Minimal fix:** root context from signal (`signal.NotifyContext`) and pass through DB/file workflows.
- **Test strategy:** integration test with induced lock contention and interrupt.
- **Confidence:** Medium.

## PASS 3 — Persistence & Boundary

### Boundary Inventory
- **DB writes:** `enqueueFromStdin` (UPSERT), `claimCmd` (UPDATE...RETURNING), `markDone` (UPDATE), `resetCmd` (DELETE).
- **DB schema/migration boundary:** `openDB` executes schema + ALTERs.
- **Filesystem boundaries:** `fileHash`, `dirHash`, `contentHash`.

### Findings

1) **Migration errors are ignored, deferring failure to runtime command paths**  
**Severity:** Medium
- **Evidence:** `openDB()` ignores errors from both `ALTER TABLE ... claimed_at/claimed_by` statements.
- **Mechanism:** schema may remain partially migrated while command proceeds.
- **Scenario:** read-only/corrupt DB: open succeeds, later `claim` fails with missing columns in production run.
- **Minimal fix:** treat non-"duplicate column" ALTER errors as fatal.
- **Test plan:** open old schema under read-only constraints and assert explicit startup failure.
- **Confidence:** Medium.

2) **Directory hashing ignores subdirectories/symlinks, allowing missed change detection**  
**Severity:** Medium (Plausible)
- **Evidence:** `dirHash()` hashes immediate regular files only; skips nested dirs/symlinks by design.
- **Mechanism:** meaningful nested content changes do not change `content_hash`, so completed entries can remain done.
- **Scenario:** package logic moves into subdir; queue never reactivates job despite effective content change.
- **Minimal fix:** recursive walk with stable ordering (or document + enforce flat-dir invariant).
- **Test plan:** modify nested file and assert hash/reactivation behavior.
- **Confidence:** Medium.

3) **`resetCmd` ignores `RowsAffected` error on delete result**  
**Severity:** Low
- **Evidence:** `n, _ := res.RowsAffected()` discards error.
- **Mechanism:** can report incorrect deletion count under driver/result edge cases.
- **Scenario:** operator sees misleading "deleted N entries" during operational troubleshooting.
- **Minimal fix:** handle and report `RowsAffected` errors.
- **Test plan:** mock/driver simulation returning RowsAffected error.
- **Confidence:** Medium.
