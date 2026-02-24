# next

Deterministic, resumable job queue for file-by-file processing. Pipe-friendly, SQLite-backed, zero dependencies.

## Install

```bash
go build -o next .
cp next ~/bin/next
```

## Usage

```bash
# Queue files
find . -name '*.go' | next enqueue --treatment=lint

# Claim next task
next claim --treatment=lint

# Mark complete
next done --path=foo.go --result=abc123 --revisit='14 days'

# Check status
next status

# Reset treatment
next reset --treatment=lint --yes
```

## Design

**Hash-ordered:** Files processed in deterministic order (sha256 of path)  
**Cursor-based:** Resume with `--cursor=HASH` (no offset drift)  
**Content-aware:** Re-enqueue on file change (content hash in PK)  
**Revisit:** Schedule periodic re-checks with `--revisit`

## Schema

```sql
queue(path, path_hash, content_hash, treatment, done_at, result, next_at)
```

Queue = `done_at IS NULL`  
Done = `done_at IS NOT NULL`  
Due = `next_at < NOW()`

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
- **No coordination needed** - each worker has a disjoint hash range
- **Deterministic distribution** - same file always goes to same shard
- **Simple scaling** - add workers by increasing shard count
- **Efficient** - no database contention between shards

### Using cursors (legacy)

```bash
# Worker loop
CURSOR=""
while path=$(next claim --treatment=lint --cursor="$CURSOR" --n=1); do
  [ -z "$path" ] && break
  # Process $path
  result=$(./check "$path" | shasum -a 256)
  next done --path="$path" --result="$result"
  CURSOR=$(echo -n "$path" | shasum -a 256 | awk '{print $1}')
done
```

Use sharding instead for true parallelism without coordination overhead.
