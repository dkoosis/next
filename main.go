package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
)

//go:embed schema.sql
var embeddedSchema string

const defaultDBPath = ".quality/ledger.db"

// ErrNoQueueEntry is returned by markDone when no row matches the given path/treatment.
var ErrNoQueueEntry = errors.New("no matching queue entry")

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	switch os.Args[1] {
	case "enqueue":
		enqueueCmd()
	case "claim":
		claimCmd()
	case "done":
		doneCmd()
	case "status":
		statusCmd()
	case "reset":
		resetCmd()
	case "help", "--help", "-h":
		usage()
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `next — deterministic job queue for file-by-file processing

Lifecycle:
  1. enqueue  pipe paths into the queue (stdin, one per line)
  2. claim    atomically take N unclaimed paths (prints to stdout)
  3.          process the claimed files externally
  4. done     mark each path complete; optionally schedule revisit

Concepts:
  treatment    Named processing pass (e.g. "lint", "format"). Same path can
               have independent entries per treatment.
  path_hash    SHA-256 of absolute path. Deterministic ordering, sharding, cursoring.
  content_hash SHA-256 of file contents. Changed content re-enqueues the entry.
  lease        claim sets a time-limited lease. Expired leases become reclaimable.
  revisit      done --revisit schedules the entry to reappear after a duration.

Commands:
  enqueue   Read paths from stdin, upsert into queue
  claim     Atomically claim unclaimed path(s), print to stdout
  done      Mark a path as complete (silent on success)
  status    Show pending/done counts per treatment
  reset     Delete all entries for a treatment (destructive)

Per-command help: next <command> --help

Common flags (all commands):
  --db         Database path (default: .quality/ledger.db)
  --treatment  Treatment name (default: "default")

Machine output: claim and status support --json for structured output.
Exit codes: 0 success, 1 error (message on stderr).

Examples:
  find . -name '*.go' | next enqueue --treatment=lint
  next claim --treatment=lint --n=10 --json
  next done --path=foo.go --result=abc123
  next done --path=bar.go --revisit='+14 days'
  next status --json
  next reset --treatment=lint --yes

Parallel processing (sharding divides the hash space across workers):
  next claim --treatment=lint --shard=0 --total-shards=4 --n=100
  next claim --treatment=lint --shard=1 --total-shards=4 --n=100
`)
}

// ----------------------------------------
// Hash utilities
// ----------------------------------------

func pathHash(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// calculateShardRange returns the hash range [start, end) for the given shard.
// SHA256 hashes are 64 hex chars (256 bits). We divide the space evenly.
func calculateShardRange(shard, totalShards int) (startHex, endHex string) {
	const maxVal = uint64(0xFFFFFFFFFFFFFFFF)

	shardSize := maxVal / uint64(totalShards) // #nosec G115 -- totalShards is validated positive before call
	start := uint64(shard) * shardSize        // #nosec G115 -- shard is validated non-negative before call
	var end uint64
	if shard == totalShards-1 {
		// Last shard goes to the end.
		end = maxVal
	} else {
		end = start + shardSize
	}

	// Convert to hex strings, padded to 64 chars total (16 hex + 48 zeros).
	startHex = fmt.Sprintf("%016x", start) + strings.Repeat("0", 48)
	endHex = fmt.Sprintf("%016x", end) + strings.Repeat("0", 48)

	return startHex, endHex
}

func fileHash(path string) (string, error) {
	f, err := os.Open(path) // #nosec G304 — path is user-provided by design
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// contentHash returns a SHA-256 hex digest for the given path.
// If path is a directory, it delegates to dirHash; otherwise to fileHash.
func contentHash(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if info.IsDir() {
		return dirHash(path)
	}
	return fileHash(path)
}

// dirHash computes a content hash for a directory by hashing the sorted
// list of (filename, content-hash) pairs for immediate regular files only.
// Subdirectories and symlinks are skipped — this targets Go package-level granularity.
// Note: all empty directories produce the same hash (SHA256 of empty input);
// this is acceptable because path_hash is the primary key discriminator.
func dirHash(path string) (string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return "", err
	}

	h := sha256.New()
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		fh, err := fileHash(filepath.Join(path, entry.Name()))
		if err != nil {
			return "", fmt.Errorf("hash %s: %w", entry.Name(), err)
		}
		// Write "name\0hash\n" for each file — sorted by os.ReadDir guarantee.
		fmt.Fprintf(h, "%s\x00%s\n", entry.Name(), fh)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// ----------------------------------------
// DB
// ----------------------------------------

func openDB(path string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA busy_timeout=3000",
		"PRAGMA foreign_keys=ON",
	} {
		if _, execErr := db.ExecContext(ctx, pragma); execErr != nil {
			_ = db.Close()
			return nil, execErr
		}
	}

	if _, execErr := db.ExecContext(ctx, embeddedSchema); execErr != nil {
		_ = db.Close()
		return nil, fmt.Errorf("schema execution failed: %w", execErr)
	}

	// Migrate: add claim columns if missing (idempotent).
	for _, col := range []string{"claimed_at", "claimed_by"} {
		_, alterErr := db.ExecContext(ctx, `ALTER TABLE queue ADD COLUMN `+col+` TEXT`)
		if alterErr != nil && !strings.Contains(alterErr.Error(), "duplicate column") {
			_ = db.Close()
			return nil, fmt.Errorf("migration (add %s): %w", col, alterErr)
		}
	}

	// Index on claimed_at — created after migration so it works on upgraded DBs.
	if _, execErr := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_queue_claimed_at ON queue(claimed_at)`); execErr != nil {
		_ = db.Close()
		return nil, fmt.Errorf("index creation failed: %w", execErr)
	}

	return db, nil
}

// writeJSON encodes v as indented JSON to stdout.
func writeJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		fmt.Fprintf(os.Stderr, "json encode error: %v\n", err)
		os.Exit(1)
	}
}

// fatal prints to stderr and exits.
func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

// ----------------------------------------
// enqueue
// ----------------------------------------

// upsertSQL is the UPSERT statement for enqueue. Content-change detection
// re-activates a job by clearing done_at/result/next_at when the hash differs.
//
//nolint:dupword // SQL NULL repetition is intentional
const upsertSQL = `
	INSERT INTO queue
	  (path, path_hash, content_hash, treatment, done_at, result, next_at)
	VALUES (?, ?, ?, ?, NULL, NULL, NULL)
	ON CONFLICT(path_hash, treatment) DO UPDATE SET
	  path         = excluded.path,
	  content_hash = excluded.content_hash,
	  done_at      = IIF(queue.content_hash = excluded.content_hash, queue.done_at, NULL),
	  result       = IIF(queue.content_hash = excluded.content_hash, queue.result, NULL),
	  next_at      = IIF(queue.content_hash = excluded.content_hash, queue.next_at, NULL)
`

func enqueueCmd() {
	fs := flag.NewFlagSet("enqueue", flag.ExitOnError)
	treatment := fs.String("treatment", "default", "treatment name")
	dbPath := fs.String("db", defaultDBPath, "database path")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: next enqueue [--treatment=NAME] [--db=PATH]

Read file paths from stdin (one per line), upsert into the queue.
Paths are converted to absolute. Empty lines are skipped.
If a path's content hash changed since last enqueue, it is re-enqueued (done_at cleared).

Stdin:  one file path per line
Stdout: "enqueued N paths for treatment=NAME"
Stderr: warnings for unreadable paths (skipped, processing continues)

Flags:
`)
		fs.PrintDefaults()
	}
	_ = fs.Parse(os.Args[2:])

	db, err := openDB(*dbPath)
	if err != nil {
		fatal("db error: %v", err)
	}

	count, err := enqueueFromStdin(db, *treatment)
	if err != nil {
		_ = db.Close()
		fatal("enqueue: %v", err)
	}
	fmt.Printf("enqueued %d paths for treatment=%s\n", count, *treatment)
	_ = db.Close()
}

func enqueueFromStdin(db *sql.DB, treatment string) (int, error) {
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}

	stmt, err := tx.PrepareContext(ctx, upsertSQL)
	if err != nil {
		_ = tx.Rollback()
		return 0, fmt.Errorf("prepare: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	scanner := bufio.NewScanner(os.Stdin)
	count := 0
	for scanner.Scan() {
		path := scanner.Text()
		if path == "" {
			continue
		}
		absPath, err := filepath.Abs(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: skipping %q: %v\n", path, err)
			continue
		}
		ph := pathHash(absPath)
		ch, err := contentHash(absPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: skipping %q: %v\n", absPath, err)
			continue
		}

		if _, err := stmt.ExecContext(ctx, absPath, ph, ch, treatment); err != nil {
			_ = tx.Rollback()
			return 0, fmt.Errorf("insert %q: %w", absPath, err)
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		_ = tx.Rollback()
		return 0, fmt.Errorf("reading stdin: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return count, nil
}

// ----------------------------------------
// claim
// ----------------------------------------

// ClaimResult holds a claimed path and its hash.
type ClaimResult struct {
	Path     string `json:"path"`
	PathHash string `json:"path_hash,omitempty"`
}

// validTimeModifier returns true if s is a valid SQLite time modifier like "5 minutes" or "+14 days".
func validTimeModifier(s string) bool {
	s = strings.TrimPrefix(s, "+")
	s = strings.TrimPrefix(s, "-")
	for _, suffix := range []string{"seconds", "minutes", "hours", "days"} {
		prefix := strings.TrimSuffix(s, " "+suffix)
		if prefix != s {
			if n, err := strconv.Atoi(prefix); err == nil && n > 0 {
				return true
			}
		}
	}
	return false
}

// validLease returns true if s is a valid positive SQLite time modifier like "5 minutes".
func validLease(s string) bool {
	if strings.HasPrefix(s, "-") {
		return false
	}
	return validTimeModifier(s)
}

// normalizeLease strips the "+" prefix so the value is safe for
// SQL construction like DATETIME('now', '-' || lease).
func normalizeLease(s string) string {
	return strings.TrimPrefix(s, "+")
}

func claimCmd() {
	fs := flag.NewFlagSet("claim", flag.ExitOnError)
	treatment := fs.String("treatment", "default", "treatment name")
	cursor := fs.String("cursor", "", "resume after this path_hash")
	n := fs.Int("n", 1, "number to claim")
	dbPath := fs.String("db", defaultDBPath, "database path")
	withHash := fs.Bool("with-hash", false, "print 'hash<TAB>path' for easier cursoring")
	jsonOutput := fs.Bool("json", false, "output as JSON array")
	shard := fs.Int("shard", -1, "shard number (0-based, requires --total-shards)")
	totalShards := fs.Int("total-shards", 0, "total number of shards")
	worker := fs.String("worker", "", "worker identifier for lease tracking")
	lease := fs.String("lease", "5 minutes", "lease duration (SQLite modifier, e.g. '5 minutes')")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: next claim [--treatment=NAME] [--n=N] [--json] [flags]

Atomically claim unclaimed paths and print them to stdout.
Claims paths that are: not done OR past revisit time, AND lease expired or unclaimed.
Ordered by path_hash for deterministic cursor-based pagination.

Output (default):     one absolute path per line
Output (--with-hash): PATH_HASH<TAB>PATH per line (use hash as --cursor for next batch)
Output (--json):      [{"path":"...","path_hash":"..."}]

Empty output (no lines / empty JSON array []) means nothing left to claim.

Sharding: --shard and --total-shards partition the hash space for parallel workers.
Cursoring: --cursor=HASH resumes after that hash (use with --with-hash output).
Lease: claimed paths expire after --lease duration and become reclaimable.

Flags:
`)
		fs.PrintDefaults()
	}
	_ = fs.Parse(os.Args[2:])

	validateClaimFlags(*n, *shard, *totalShards)

	// Default --worker to hostname:pid for debuggability.
	if *worker == "" {
		host, _ := os.Hostname()
		*worker = fmt.Sprintf("%s:%d", host, os.Getpid())
	}

	// Validate --lease is a reasonable SQLite time modifier.
	if !validLease(*lease) {
		fatal("error: --lease must be like '5 minutes', '1 hours', '7 days'")
	}

	db, err := openDB(*dbPath)
	if err != nil {
		fatal("db error: %v", err)
	}
	defer func() { _ = db.Close() }()

	query, args := buildClaimQuery(*treatment, *cursor, *worker, normalizeLease(*lease), *n, *shard, *totalShards)

	rows, err := db.QueryContext(context.Background(), query, args...)
	if err != nil {
		fatal("query error: %v", err)
	}
	defer func() { _ = rows.Close() }()

	results := scanClaimResults(rows)
	writeClaimResults(results, *jsonOutput, *withHash)
}

func scanClaimResults(rows *sql.Rows) []ClaimResult {
	results := []ClaimResult{}
	for rows.Next() {
		var path, hash string
		if err := rows.Scan(&path, &hash); err != nil {
			fmt.Fprintf(os.Stderr, "warning: scan error: %v\n", err)
			continue
		}
		results = append(results, ClaimResult{Path: path, PathHash: hash})
	}
	if err := rows.Err(); err != nil {
		fatal("rows error: %v", err)
	}
	return results
}

func validateClaimFlags(n, shard, totalShards int) {
	if n <= 0 {
		fatal("error: --n must be a positive integer")
	}
	if (shard >= 0 && totalShards <= 0) || (shard < 0 && totalShards > 0) {
		fatal("error: --shard and --total-shards must be used together")
	}
	if shard >= totalShards {
		fatal("error: --shard must be less than --total-shards")
	}
}

func buildClaimQuery(treatment, cursor, worker, lease string, n, shard, totalShards int) (string, []any) {
	// Inner SELECT finds eligible rows.
	inner := `
		SELECT rowid FROM queue
		 WHERE treatment = ?
		   AND (done_at IS NULL OR (next_at IS NOT NULL AND next_at <= DATETIME('now')))
		   AND (claimed_at IS NULL OR claimed_at <= DATETIME('now', '-' || ?))
		   AND path_hash > ?`
	args := []any{treatment, lease, cursor}

	if shard >= 0 {
		shardStart, shardEnd := calculateShardRange(shard, totalShards)
		inner += `
		   AND path_hash >= ?
		   AND path_hash < ?`
		args = append(args, shardStart, shardEnd)
	}

	inner += `
		 ORDER BY path_hash
		 LIMIT ?`
	args = append(args, n)

	// Outer UPDATE atomically claims those rows.
	query := fmt.Sprintf(`
		UPDATE queue
		   SET claimed_at = DATETIME('now'), claimed_by = ?
		 WHERE rowid IN (%s)
		 RETURNING path, path_hash`, inner)
	args = append([]any{worker}, args...)

	return query, args
}

func writeClaimResults(results []ClaimResult, jsonOutput, withHash bool) {
	if jsonOutput {
		writeJSON(results)
		return
	}
	for _, r := range results {
		if withHash {
			fmt.Printf("%s\t%s\n", r.PathHash, r.Path)
		} else {
			fmt.Println(r.Path)
		}
	}
}

// ----------------------------------------
// done
// ----------------------------------------

func doneCmd() {
	fs := flag.NewFlagSet("done", flag.ExitOnError)
	path := fs.String("path", "", "file path (required)")
	result := fs.String("result", "", "result hash")
	revisit := fs.String("revisit", "", "revisit after duration (e.g., '+14 days')")
	treatment := fs.String("treatment", "default", "treatment name")
	dbPath := fs.String("db", defaultDBPath, "database path")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: next done --path=FILE [--result=HASH] [--revisit=DURATION] [flags]

Mark a path as complete. Clears its lease.
Fails with exit 1 if no matching entry exists for path + treatment.

--path is required (relative paths are resolved to absolute).
--revisit schedules re-entry after a duration: "+14 days", "1 hours", "30 minutes".
--result stores an arbitrary string (e.g. content hash of output).

Stdout: silent on success
Stderr: error message on failure (exit 1)

Flags:
`)
		fs.PrintDefaults()
	}
	_ = fs.Parse(os.Args[2:])

	if *path == "" {
		fatal("error: --path required")
	}
	if *revisit != "" && !validTimeModifier(*revisit) {
		fatal("error: --revisit must be like '+14 days', '1 hours', '30 minutes'")
	}
	absPath, err := filepath.Abs(*path)
	if err != nil {
		fatal("path error: %v", err)
	}
	ph := pathHash(absPath)

	db, err := openDB(*dbPath)
	if err != nil {
		fatal("db error: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := markDone(db, ph, *treatment, *result, *revisit); err != nil {
		fatal("%v", err)
	}
}

// markDone marks a queue entry as complete. Returns an error if no matching row exists.
func markDone(db *sql.DB, pathHash, treatment, result, revisit string) error {
	ctx := context.Background()
	now := time.Now().UTC().Format("2006-01-02 15:04:05")

	// When revisit is empty the IIF collapses to NULL; otherwise it computes the next visit time.
	res, err := db.ExecContext(ctx, `
		UPDATE queue
		   SET done_at = ?, result = ?,
		       next_at = IIF(? = '', NULL, DATETIME('now', ?)),
		       claimed_at = NULL, claimed_by = NULL
		 WHERE path_hash = ? AND treatment = ?
	`, now, result, revisit, revisit, pathHash, treatment)
	if err != nil {
		return fmt.Errorf("update error: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("path_hash=%s treatment=%s: %w", pathHash, treatment, ErrNoQueueEntry)
	}
	return nil
}

// ----------------------------------------
// status
// ----------------------------------------

// StatusResult holds per-treatment queue statistics.
type StatusResult struct {
	Treatment string `json:"treatment"`
	Pending   int    `json:"pending"`
	Done      int    `json:"done"`
}

func statusCmd() {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	treatment := fs.String("treatment", "", "filter by treatment (empty = all)")
	dbPath := fs.String("db", defaultDBPath, "database path")
	jsonOutput := fs.Bool("json", false, "output as JSON array")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: next status [--treatment=NAME] [--json] [--db=PATH]

Show pending/done counts per treatment.

Output (default): table with columns TREATMENT, PENDING, DONE (includes TOTAL row)
Output (--json):  [{"treatment":"...","pending":N,"done":N}] (TOTAL row included)

When --treatment is set, shows only that treatment (still includes TOTAL row).

Flags:
`)
		fs.PrintDefaults()
	}
	_ = fs.Parse(os.Args[2:])

	db, err := openDB(*dbPath)
	if err != nil {
		fatal("db error: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()

	query := `
WITH stats AS (
  SELECT treatment,
         SUM(CASE WHEN done_at IS NULL THEN 1 ELSE 0 END) AS pending,
         SUM(CASE WHEN done_at IS NOT NULL THEN 1 ELSE 0 END) AS done
    FROM queue
`
	args := []any{}
	if *treatment != "" {
		query += "   WHERE treatment = ?\n"
		args = append(args, *treatment)
	}
	query += "GROUP BY treatment)\n" +
		"SELECT treatment, pending, done, 0 AS sort_order FROM stats\n" +
		"UNION ALL\n" +
		"SELECT 'TOTAL', COALESCE(SUM(pending), 0), COALESCE(SUM(done), 0), 1 FROM stats\n" +
		"ORDER BY sort_order, treatment;"

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		fatal("query error: %v", err)
	}
	defer func() { _ = rows.Close() }()

	results := []StatusResult{}
	for rows.Next() {
		var t string
		var pending, done, sortOrder int
		if err := rows.Scan(&t, &pending, &done, &sortOrder); err != nil {
			fmt.Fprintf(os.Stderr, "warning: scan error: %v\n", err)
			continue
		}
		results = append(results, StatusResult{Treatment: t, Pending: pending, Done: done})
	}
	if err := rows.Err(); err != nil {
		fatal("rows error: %v", err)
	}

	if *jsonOutput {
		writeJSON(results)
	} else {
		fmt.Printf("%-20s %10s %10s\n", "TREATMENT", "PENDING", "DONE")
		for _, r := range results {
			fmt.Printf("%-20s %10d %10d\n", r.Treatment, r.Pending, r.Done)
		}
	}
}

// ----------------------------------------
// reset
// ----------------------------------------

func resetCmd() {
	fs := flag.NewFlagSet("reset", flag.ExitOnError)
	treatment := fs.String("treatment", "", "treatment to reset (required)")
	dbPath := fs.String("db", defaultDBPath, "database path")
	confirm := fs.Bool("yes", false, "skip confirmation")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: next reset --treatment=NAME [--yes] [--db=PATH]

Delete ALL queue entries for the given treatment. Destructive and irreversible.
Prompts for confirmation unless --yes is passed. Always use --yes in scripts.

--treatment is required.

Stdout: "deleted N entries"

Flags:
`)
		fs.PrintDefaults()
	}
	_ = fs.Parse(os.Args[2:])

	if *treatment == "" {
		fatal("error: --treatment required")
	}
	if !*confirm {
		fmt.Printf("Delete all entries for treatment=%s? [y/N] ", *treatment)
		var response string
		_, _ = fmt.Scanln(&response)
		if response != "y" && response != "Y" {
			fmt.Println("canceled")
			return
		}
	}

	db, err := openDB(*dbPath)
	if err != nil {
		fatal("db error: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	res, err := db.ExecContext(ctx, "DELETE FROM queue WHERE treatment = ?", *treatment)
	if err != nil {
		fatal("delete error: %v", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		fatal("rows affected: %v", err)
	}
	fmt.Printf("deleted %d entries\n", n)
}
