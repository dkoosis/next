# next

Deterministic, resumable job queue for file-by-file processing. Pipe-friendly, SQLite-backed, zero dependencies.

## Install

```bash
go install github.com/dkoosis/next@latest
```

## Usage

```bash
# Queue files or directories
find . -name '*.go' | next enqueue --treatment=lint
go list -f '{{.Dir}}' ./... | next enqueue --treatment=craft

# Claim next task
next claim --treatment=lint

# Mark complete (optionally schedule revisit)
next done --path=foo.go --result=abc123
next done --path=bar.go --result=def456 --revisit='+14 days'

# Check status
next status
next status --treatment=lint --json

# Reset treatment (destructive)
next reset --treatment=lint --yes
```

## Design

**Hash-ordered:** Files processed in deterministic order (SHA-256 of absolute path).
**Cursor-based:** Resume with `--cursor=HASH` (no offset drift).
**Content-aware:** Re-enqueue detects file changes via content hash — changed files are reactivated even if previously done.
**Directory-aware:** Enqueue accepts directories (e.g. Go package dirs). Content hash covers all regular files in the directory.
**Leased claims:** `claim` sets a time-limited lease (default 5 minutes). Expired leases become reclaimable, preventing stuck jobs from interrupted workers.
**Revisit:** Schedule periodic re-checks with `--revisit='+14 days'`.

## Schema

```sql
queue(path, path_hash, content_hash, treatment, done_at, result, next_at, claimed_at, claimed_by)
```

Pending = `done_at IS NULL`
Done = `done_at IS NOT NULL`
Due for revisit = `next_at <= NOW()`
Lease expired = `claimed_at <= NOW() - lease`

## Resuming interrupted work

Re-running `enqueue` with the same paths is always safe:

- **Changed content** → `done_at` cleared, entry becomes pending again
- **Unchanged content** → stays done (no redundant re-processing)
- **Expired claims** → reclaimable after lease timeout (default 5 min)

This makes `next` idempotent for batch workflows that may be interrupted and resumed later.

## Parallel workers

### Using sharding (recommended)

Sharding divides the hash space evenly across workers without coordination:

```bash
# Worker 1 (processes shard 0 of 4)
while true; do
  next claim --treatment=lint --shard=0 --total-shards=4 --n=10 | while read path; do
    [ -z "$path" ] && break
    result=$(./check "$path" | shasum -a 256)
    next done --path="$path" --result="$result"
  done
  sleep 1
done

# Worker 2 (processes shard 1 of 4)
while true; do
  next claim --treatment=lint --shard=1 --total-shards=4 --n=10 | while read path; do
    [ -z "$path" ] && break
    result=$(./check "$path" | shasum -a 256)
    next done --path="$path" --result="$result"
  done
  sleep 1
done
```

Sharding benefits:
- **No coordination needed** — each worker has a disjoint hash range
- **Deterministic distribution** — same file always goes to same shard
- **Simple scaling** — add workers by increasing shard count
- **Efficient** — no database contention between shards

### Using leases

Claims are automatically leased to prevent double-processing:

```bash
# Worker with custom lease and identity
next claim --treatment=lint --n=5 --worker=build-server-1 --lease='10 minutes'
```

If a worker crashes, its claimed items become reclaimable after the lease expires.

### Using cursors

```bash
CURSOR=""
while path=$(next claim --treatment=lint --cursor="$CURSOR" --n=1); do
  [ -z "$path" ] && break
  result=$(./check "$path" | shasum -a 256)
  next done --path="$path" --result="$result"
  CURSOR=$(echo -n "$path" | shasum -a 256 | awk '{print $1}')
done
```
