package main

import (
	"bufio"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

const defaultDB = ".quality/ledger.db"

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
  enqueue   Read paths from stdin, add to queue
  claim     Claim next unclaimed path(s)
  done      Mark path as complete
  status    Show queue stats
  reset     Clear treatment from queue

Examples:
  find . -name '*.go' | next enqueue --treatment=lint
  next claim --treatment=lint
  next done --path=foo.go --result=abc123
`)
}

// Hash utilities
func pathHash(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func fileHash(path string) (string, error) {
	f, err := os.Open(path)
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

func openDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// Ensure schema
	if _, err := db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		db.Close()
		return nil, err
	}
	schema, err := os.ReadFile(".quality/schema.sql")
	if err == nil {
		db.Exec(string(schema))
	}
	return db, nil
}

func enqueueCmd() {
	fs := flag.NewFlagSet("enqueue", flag.ExitOnError)
	treatment := fs.String("treatment", "default", "treatment name")
	dbPath := fs.String("db", defaultDB, "database path")
	fs.Parse(os.Args[2:])

	db, err := openDB(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	scanner := bufio.NewScanner(os.Stdin)
	count := 0
	for scanner.Scan() {
		path := scanner.Text()
		if path == "" {
			continue
		}
		// Make path absolute for consistency
		absPath, err := filepath.Abs(path)
		if err != nil {
			continue
		}
		ph := pathHash(absPath)
		ch, err := fileHash(absPath)
		if err != nil {
			continue // skip unreadable files
		}
		_, err = db.Exec(`
			INSERT OR IGNORE INTO queue 
			(path, path_hash, content_hash, treatment, done_at, result, next_at)
			VALUES (?, ?, ?, ?, NULL, NULL, NULL)
		`, absPath, ph, ch, *treatment)
		if err == nil {
			count++
		}
	}
	fmt.Printf("enqueued %d paths for treatment=%s\n", count, *treatment)
}

func claimCmd() {
	fs := flag.NewFlagSet("claim", flag.ExitOnError)
	treatment := fs.String("treatment", "default", "treatment name")
	cursor := fs.String("cursor", "", "resume after this path_hash")
	n := fs.Int("n", 1, "number to claim")
	dbPath := fs.String("db", defaultDB, "database path")
	fs.Parse(os.Args[2:])

	db, err := openDB(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	rows, err := db.Query(`
		SELECT path, path_hash FROM queue
		WHERE treatment=? AND done_at IS NULL AND path_hash > ?
		ORDER BY path_hash
		LIMIT ?
	`, *treatment, *cursor, *n)
	if err != nil {
		fmt.Fprintf(os.Stderr, "query error: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()

	for rows.Next() {
		var path, hash string
		if err := rows.Scan(&path, &hash); err != nil {
			continue
		}
		fmt.Println(path)
	}
}

func doneCmd() {
	fs := flag.NewFlagSet("done", flag.ExitOnError)
	path := fs.String("path", "", "file path (required)")
	result := fs.String("result", "", "result hash")
	revisit := fs.String("revisit", "", "revisit after duration (e.g., '14 days')")
	treatment := fs.String("treatment", "default", "treatment name")
	dbPath := fs.String("db", defaultDB, "database path")
	fs.Parse(os.Args[2:])

	if *path == "" {
		fmt.Fprintf(os.Stderr, "error: --path required\n")
		os.Exit(1)
	}

	absPath, err := filepath.Abs(*path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "path error: %v\n", err)
		os.Exit(1)
	}

	db, err := openDB(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	now := time.Now().UTC().Format(time.RFC3339)
	var nextAt *string
	if *revisit != "" {
		// SQLite datetime modifier
		nextAt = revisit
	}

	_, err = db.Exec(`
		UPDATE queue
		SET done_at=?, result=?, next_at=DATETIME('now', ?)
		WHERE path=? AND treatment=?
	`, now, *result, nextAt, absPath, *treatment)
	if err != nil {
		fmt.Fprintf(os.Stderr, "update error: %v\n", err)
		os.Exit(1)
	}
}

func statusCmd() {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	treatment := fs.String("treatment", "", "filter by treatment (empty = all)")
	dbPath := fs.String("db", defaultDB, "database path")
	fs.Parse(os.Args[2:])

	db, err := openDB(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	query := `
		SELECT treatment, 
		       COUNT(*) FILTER (WHERE done_at IS NULL) as pending,
		       COUNT(*) FILTER (WHERE done_at IS NOT NULL) as done
		FROM queue
	`
	args := []interface{}{}
	if *treatment != "" {
		query += " WHERE treatment=?"
		args = append(args, *treatment)
	}
	query += " GROUP BY treatment"

	rows, err := db.Query(query, args...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "query error: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()

	fmt.Printf("%-20s %10s %10s\n", "TREATMENT", "PENDING", "DONE")
	for rows.Next() {
		var t string
		var pending, done int
		rows.Scan(&t, &pending, &done)
		fmt.Printf("%-20s %10d %10d\n", t, pending, done)
	}
}

func resetCmd() {
	fs := flag.NewFlagSet("reset", flag.ExitOnError)
	treatment := fs.String("treatment", "", "treatment to reset (required)")
	dbPath := fs.String("db", defaultDB, "database path")
	confirm := fs.Bool("yes", false, "skip confirmation")
	fs.Parse(os.Args[2:])

	if *treatment == "" {
		fmt.Fprintf(os.Stderr, "error: --treatment required\n")
		os.Exit(1)
	}

	if !*confirm {
		fmt.Printf("Delete all entries for treatment=%s? [y/N] ", *treatment)
		var response string
		fmt.Scanln(&response)
		if response != "y" && response != "Y" {
			fmt.Println("cancelled")
			return
		}
	}

	db, err := openDB(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	res, err := db.Exec("DELETE FROM queue WHERE treatment=?", *treatment)
	if err != nil {
		fmt.Fprintf(os.Stderr, "delete error: %v\n", err)
		os.Exit(1)
	}
	n, _ := res.RowsAffected()
	fmt.Printf("deleted %d entries\n", n)
}
