package main

import (
	"bufio"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
)

//go:embed schema.sql
var embeddedSchemaFS embed.FS

const fallbackSchema = `
CREATE TABLE IF NOT EXISTS queue (
  path          TEXT NOT NULL,
  path_hash     TEXT NOT NULL,
  content_hash  TEXT NOT NULL,
  treatment     TEXT NOT NULL,
  done_at       TEXT,
  result        TEXT,
  next_at       TEXT,
  PRIMARY KEY (path_hash, treatment)
);
CREATE INDEX IF NOT EXISTS idx_queue_treatment_done ON queue(treatment, done_at);
CREATE INDEX IF NOT EXISTS idx_queue_next_at ON queue(next_at);
`

const defaultDBPath = ".quality/ledger.db"

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
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `next: deterministic job queue

Commands:
  enqueue     Read paths from stdin, add to queue
  claim       Claim next unclaimed path(s)
  done        Mark path as complete
  status      Show queue stats
  reset       Clear treatment from queue

Examples:
  find . -name '*.go' | next enqueue --treatment=lint
  next claim --treatment=lint --json
  next done --path=foo.go --result=abc123 --revisit='+14 days'
  next status --json
`)
}

// ----------------------------------------
// Hash utilities
// ----------------------------------------

func pathHash(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func fileHash(path string) (string, error) {
	f, err := os.Open(path) // #nosec G304 — path is user-provided by design
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// ----------------------------------------
// DB
// ----------------------------------------

func openDB(path string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	schema := readEmbeddedSchemaOrFallback()
	if _, execErr := db.Exec(schema); execErr != nil {
		_ = db.Close()
		return nil, fmt.Errorf("schema execution failed: %w", execErr)
	}

	if _, execErr := db.Exec(`PRAGMA journal_mode=WAL;`); execErr != nil {
		_ = db.Close()
		return nil, execErr
	}
	_, _ = db.Exec(`PRAGMA busy_timeout=3000;`)
	_, _ = db.Exec(`PRAGMA foreign_keys=ON;`)
	return db, nil
}

func readEmbeddedSchemaOrFallback() string {
	if b, err := embeddedSchemaFS.ReadFile("schema.sql"); err == nil && len(b) > 0 {
		return string(b)
	}
	return fallbackSchema
}

// ----------------------------------------
// enqueue
// ----------------------------------------

func enqueueCmd() {
	if err := doEnqueueCmd(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func doEnqueueCmd() error {
	fs := flag.NewFlagSet("enqueue", flag.ExitOnError)
	treatment := fs.String("treatment", "default", "treatment name")
	dbPath := fs.String("db", defaultDBPath, "database path")
	_ = fs.Parse(os.Args[2:])

	db, err := openDB(*dbPath)
	if err != nil {
		return fmt.Errorf("db error: %w", err)
	}
	defer db.Close()

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
		ch, err := fileHash(absPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: skipping %q: %v\n", absPath, err)
			continue
		}

		// UPSERT: re-activate job if content changed
		_, err = db.Exec(`
			INSERT INTO queue
			  (path, path_hash, content_hash, treatment, done_at, result, next_at)
			VALUES (?, ?, ?, ?, NULL, NULL, NULL)
			ON CONFLICT(path_hash, treatment) DO UPDATE SET
			  path         = excluded.path,
			  content_hash = excluded.content_hash,
			  done_at      = IIF(queue.content_hash = excluded.content_hash, queue.done_at, NULL),
			  result       = IIF(queue.content_hash = excluded.content_hash, queue.result, NULL),
			  next_at      = IIF(queue.content_hash = excluded.content_hash, queue.next_at, NULL)
		`, absPath, ph, ch, *treatment)
		if err != nil {
			return fmt.Errorf("insert failed for %q: %w", absPath, err)
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading stdin: %w", err)
	}
	fmt.Printf("enqueued %d paths for treatment=%s\n", count, *treatment)
	return nil
}

// ----------------------------------------
// claim
// ----------------------------------------

type ClaimResult struct {
	Path     string `json:"path"`
	PathHash string `json:"path_hash,omitempty"`
}

func claimCmd() {
	fs := flag.NewFlagSet("claim", flag.ExitOnError)
	treatment := fs.String("treatment", "default", "treatment name")
	cursor := fs.String("cursor", "", "resume after this path_hash")
	n := fs.Int("n", 1, "number to claim")
	dbPath := fs.String("db", defaultDBPath, "database path")
	withHash := fs.Bool("with-hash", false, "print 'hash<TAB>path' for easier cursoring")
	jsonOutput := fs.Bool("json", false, "output as JSON array")
	_ = fs.Parse(os.Args[2:])

	db, err := openDB(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	rows, err := db.Query(`
		SELECT path, path_hash
		  FROM queue
		 WHERE treatment = ?
		   AND done_at IS NULL
		   AND path_hash > ?
		 ORDER BY path_hash
		 LIMIT ?
	`, *treatment, *cursor, *n)
	if err != nil {
		fmt.Fprintf(os.Stderr, "query error: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()

	var results []ClaimResult
	for rows.Next() {
		var path, hash string
		if err := rows.Scan(&path, &hash); err != nil {
			continue
		}
		results = append(results, ClaimResult{Path: path, PathHash: hash})
	}
	if err := rows.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "rows error: %v\n", err)
		os.Exit(1)
	}

	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(results)
	} else {
		for _, r := range results {
			if *withHash {
				fmt.Printf("%s\t%s\n", r.PathHash, r.Path)
			} else {
				fmt.Println(r.Path)
			}
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
	_ = fs.Parse(os.Args[2:])

	if *path == "" {
		fmt.Fprintf(os.Stderr, "error: --path required\n")
		os.Exit(1)
	}
	absPath, err := filepath.Abs(*path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "path error: %v\n", err)
		os.Exit(1)
	}
	ph := pathHash(absPath)

	db, err := openDB(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	now := time.Now().UTC().Format(time.RFC3339)

	if *revisit == "" {
		_, err = db.Exec(`
			UPDATE queue
			   SET done_at = ?, result = ?, next_at = NULL
			 WHERE path_hash = ? AND treatment = ?
		`, now, *result, ph, *treatment)
	} else {
		_, err = db.Exec(`
			UPDATE queue
			   SET done_at = ?, result = ?, next_at = DATETIME('now', ?)
			 WHERE path_hash = ? AND treatment = ?
		`, now, *result, *revisit, ph, *treatment)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "update error: %v\n", err)
		os.Exit(1)
	}
}

// ----------------------------------------
// status
// ----------------------------------------

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
	_ = fs.Parse(os.Args[2:])

	db, err := openDB(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

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
		"SELECT treatment, pending, done FROM stats\n" +
		"UNION ALL\n" +
		"SELECT 'TOTAL', SUM(pending), SUM(done) FROM stats\n" +
		"ORDER BY (treatment = 'TOTAL'), treatment;"

	rows, err := db.Query(query, args...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "query error: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()

	var results []StatusResult
	for rows.Next() {
		var t string
		var pending, done int
		if err := rows.Scan(&t, &pending, &done); err != nil {
			continue
		}
		results = append(results, StatusResult{Treatment: t, Pending: pending, Done: done})
	}
	if err := rows.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "rows error: %v\n", err)
		os.Exit(1)
	}

	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(results)
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
	_ = fs.Parse(os.Args[2:])

	if *treatment == "" {
		fmt.Fprintf(os.Stderr, "error: --treatment required\n")
		os.Exit(1)
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
		fmt.Fprintf(os.Stderr, "db error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	res, err := db.Exec("DELETE FROM queue WHERE treatment = ?", *treatment)
	if err != nil {
		fmt.Fprintf(os.Stderr, "delete error: %v\n", err)
		os.Exit(1)
	}
	n, _ := res.RowsAffected()
	fmt.Printf("deleted %d entries\n", n)
}
