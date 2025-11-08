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

Use GNU parallel or separate machines - each with their own cursor.
