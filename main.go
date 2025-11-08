package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	stdErrors "errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/dkoosis/next/internal/cgerr"
	"github.com/dkoosis/next/internal/logx"

	_ "modernc.org/sqlite"
)

const (
	defaultDB      = ".quality/ledger.db"
	schemaPath     = ".quality/schema.sql"
	commandEnqueue = "enqueue"
	commandClaim   = "claim"
	commandDone    = "done"
	commandStatus  = "status"
	commandReset   = "reset"
)

func main() {
	ctx := context.Background()
	if err := run(ctx, os.Args[1:]); err != nil {
		logx.Error(ctx, "command execution failed", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		usage()
		return cgerr.NewValidationError(errors.New("missing command"), "no command provided")
	}

	cmd := args[0]
	switch cmd {
	case commandEnqueue:
		return enqueueCmd(ctx, args[1:])
	case commandClaim:
		return claimCmd(ctx, args[1:])
	case commandDone:
		return doneCmd(ctx, args[1:])
	case commandStatus:
		return statusCmd(ctx, args[1:])
	case commandReset:
		return resetCmd(ctx, args[1:])
	default:
		usage()
		return cgerr.NewValidationError(errors.Newf("unknown command %q", cmd), "unrecognized command")
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

func pathHash(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func fileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", cgerr.NewResourceError(errors.Wrapf(err, "open file"), "unable to read file").WithDetail("path", path)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil {
			logx.Warn(context.Background(), "failed to close file", cgerr.NewResourceError(errors.Wrapf(cerr, "close file"), "failed to close file").WithDetail("path", path))
		}
	}()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", cgerr.NewResourceError(errors.Wrapf(err, "hash file"), "unable to read file").WithDetail("path", path)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func openDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, cgerr.NewResourceError(errors.Wrapf(err, "open sqlite database"), "unable to open database").WithDetail("path", path)
	}

	if _, err := db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		if cerr := db.Close(); cerr != nil {
			logx.Warn(context.Background(), "failed to close database after WAL error", cgerr.NewInternalError(errors.Wrap(cerr, "close database"), "failed to close database").WithDetail("path", path))
		}
		return nil, cgerr.NewInternalError(errors.Wrap(err, "enable WAL"), "failed to configure database").WithDetail("path", path)
	}

	schema, err := os.ReadFile(schemaPath)
	if err != nil {
		if !stdErrors.Is(err, os.ErrNotExist) {
			if cerr := db.Close(); cerr != nil {
				logx.Warn(context.Background(), "failed to close database after schema read error", cgerr.NewInternalError(errors.Wrap(cerr, "close database"), "failed to close database").WithDetail("path", path))
			}
			return nil, cgerr.NewResourceError(errors.Wrap(err, "read schema file"), "failed to read schema").WithDetail("schema_path", schemaPath)
		}
	} else {
		if _, execErr := db.Exec(string(schema)); execErr != nil {
			if cerr := db.Close(); cerr != nil {
				logx.Warn(context.Background(), "failed to close database after schema apply error", cgerr.NewInternalError(errors.Wrap(cerr, "close database"), "failed to close database").WithDetail("path", path))
			}
			return nil, cgerr.NewInternalError(errors.Wrap(execErr, "apply schema"), "failed to apply schema").WithDetail("schema_path", schemaPath)
		}
	}

	return db, nil
}

func enqueueCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet(commandEnqueue, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	treatment := fs.String("treatment", "default", "treatment name")
	dbPath := fs.String("db", defaultDB, "database path")
	if err := fs.Parse(args); err != nil {
		return cgerr.NewValidationError(errors.Wrap(err, "parse enqueue flags"), "invalid enqueue arguments")
	}

	db, err := openDB(*dbPath)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := db.Close(); cerr != nil {
			logx.Warn(ctx, "failed to close database", cgerr.NewInternalError(errors.Wrap(cerr, "close database"), "failed to close database").WithDetail("path", *dbPath))
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)
	count := 0
	for scanner.Scan() {
		path := strings.TrimSpace(scanner.Text())
		if path == "" {
			continue
		}

		absPath, err := filepath.Abs(path)
		if err != nil {
			logx.Warn(ctx, "skipping path with invalid absolute path", cgerr.NewValidationError(errors.Wrapf(err, "resolve absolute path"), "invalid path").WithDetail("path", path))
			continue
		}

		ph := pathHash(absPath)
		ch, err := fileHash(absPath)
		if err != nil {
			logx.Warn(ctx, "skipping unreadable file", err, "path", absPath)
			continue
		}

		_, err = db.Exec(`
                        INSERT OR IGNORE INTO queue
                        (path, path_hash, content_hash, treatment, done_at, result, next_at)
                        VALUES (?, ?, ?, ?, NULL, NULL, NULL)
                `, absPath, ph, ch, *treatment)
		if err != nil {
			logx.Error(ctx, "failed to enqueue path", cgerr.NewInternalError(errors.Wrap(err, "insert queue entry"), "failed to enqueue path").WithDetail("path", absPath).WithDetail("treatment", *treatment))
			continue
		}
		count++
	}

	if err := scanner.Err(); err != nil {
		return cgerr.NewInternalError(errors.Wrap(err, "scan stdin"), "failed to read paths from input")
	}

	fmt.Printf("enqueued %d paths for treatment=%s\n", count, *treatment)
	return nil
}

func claimCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet(commandClaim, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	treatment := fs.String("treatment", "default", "treatment name")
	cursor := fs.String("cursor", "", "resume after this path_hash")
	n := fs.Int("n", 1, "number to claim")
	dbPath := fs.String("db", defaultDB, "database path")
	if err := fs.Parse(args); err != nil {
		return cgerr.NewValidationError(errors.Wrap(err, "parse claim flags"), "invalid claim arguments")
	}

	db, err := openDB(*dbPath)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := db.Close(); cerr != nil {
			logx.Warn(ctx, "failed to close database", cgerr.NewInternalError(errors.Wrap(cerr, "close database"), "failed to close database").WithDetail("path", *dbPath))
		}
	}()

	rows, err := db.Query(`
                SELECT path, path_hash FROM queue
                WHERE treatment=? AND done_at IS NULL AND path_hash > ?
                ORDER BY path_hash
                LIMIT ?
        `, *treatment, *cursor, *n)
	if err != nil {
		return cgerr.NewInternalError(errors.Wrap(err, "query queue"), "failed to claim paths").WithDetail("treatment", *treatment)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			logx.Warn(ctx, "failed to close claim rows", cgerr.NewInternalError(errors.Wrap(cerr, "close rows"), "failed to close rows"))
		}
	}()

	for rows.Next() {
		var path, hash string
		if err := rows.Scan(&path, &hash); err != nil {
			logx.Warn(ctx, "skipping claim row", cgerr.NewInternalError(errors.Wrap(err, "scan claim row"), "failed to scan claim row").WithDetail("treatment", *treatment))
			continue
		}
		fmt.Println(path)
	}

	if err := rows.Err(); err != nil {
		return cgerr.NewInternalError(errors.Wrap(err, "iterate claim rows"), "failed to iterate claimed paths")
	}

	return nil
}

func doneCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet(commandDone, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	path := fs.String("path", "", "file path (required)")
	result := fs.String("result", "", "result hash")
	revisit := fs.String("revisit", "", "revisit after duration (e.g., '14 days')")
	treatment := fs.String("treatment", "default", "treatment name")
	dbPath := fs.String("db", defaultDB, "database path")
	if err := fs.Parse(args); err != nil {
		return cgerr.NewValidationError(errors.Wrap(err, "parse done flags"), "invalid done arguments")
	}

	if strings.TrimSpace(*path) == "" {
		return cgerr.NewValidationError(errors.New("path not provided"), "--path required")
	}

	absPath, err := filepath.Abs(*path)
	if err != nil {
		return cgerr.NewValidationError(errors.Wrap(err, "resolve absolute path"), "invalid path").WithDetail("path", *path)
	}

	db, err := openDB(*dbPath)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := db.Close(); cerr != nil {
			logx.Warn(ctx, "failed to close database", cgerr.NewInternalError(errors.Wrap(cerr, "close database"), "failed to close database").WithDetail("path", *dbPath))
		}
	}()

	now := time.Now().UTC().Format(time.RFC3339)
	var nextAt interface{}
	if strings.TrimSpace(*revisit) != "" {
		nextAt = *revisit
	} else {
		nextAt = nil
	}

	if _, err := db.Exec(`
                UPDATE queue
                SET done_at=?, result=?, next_at=DATETIME('now', ?)
                WHERE path=? AND treatment=?
        `, now, *result, nextAt, absPath, *treatment); err != nil {
		return cgerr.NewInternalError(errors.Wrap(err, "update queue entry"), "failed to mark path complete").WithDetail("path", absPath).WithDetail("treatment", *treatment)
	}

	return nil
}

func statusCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet(commandStatus, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	treatment := fs.String("treatment", "", "filter by treatment (empty = all)")
	dbPath := fs.String("db", defaultDB, "database path")
	if err := fs.Parse(args); err != nil {
		return cgerr.NewValidationError(errors.Wrap(err, "parse status flags"), "invalid status arguments")
	}

	db, err := openDB(*dbPath)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := db.Close(); cerr != nil {
			logx.Warn(ctx, "failed to close database", cgerr.NewInternalError(errors.Wrap(cerr, "close database"), "failed to close database").WithDetail("path", *dbPath))
		}
	}()

	query := `
                SELECT treatment,
                       COUNT(*) FILTER (WHERE done_at IS NULL) as pending,
                       COUNT(*) FILTER (WHERE done_at IS NOT NULL) as done
                FROM queue
        `
	argsList := []interface{}{}
	if strings.TrimSpace(*treatment) != "" {
		query += " WHERE treatment=?"
		argsList = append(argsList, *treatment)
	}
	query += " GROUP BY treatment"

	rows, err := db.Query(query, argsList...)
	if err != nil {
		return cgerr.NewInternalError(errors.Wrap(err, "query status"), "failed to query queue status")
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			logx.Warn(ctx, "failed to close status rows", cgerr.NewInternalError(errors.Wrap(cerr, "close rows"), "failed to close rows"))
		}
	}()

	fmt.Printf("%-20s %10s %10s\n", "TREATMENT", "PENDING", "DONE")
	for rows.Next() {
		var t string
		var pending, done int
		if err := rows.Scan(&t, &pending, &done); err != nil {
			return cgerr.NewInternalError(errors.Wrap(err, "scan status row"), "failed to scan status row")
		}
		fmt.Printf("%-20s %10d %10d\n", t, pending, done)
	}

	if err := rows.Err(); err != nil {
		return cgerr.NewInternalError(errors.Wrap(err, "iterate status rows"), "failed to iterate status rows")
	}

	return nil
}

func resetCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet(commandReset, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	treatment := fs.String("treatment", "", "treatment to reset (required)")
	dbPath := fs.String("db", defaultDB, "database path")
	confirm := fs.Bool("yes", false, "skip confirmation")
	if err := fs.Parse(args); err != nil {
		return cgerr.NewValidationError(errors.Wrap(err, "parse reset flags"), "invalid reset arguments")
	}

	if strings.TrimSpace(*treatment) == "" {
		return cgerr.NewValidationError(errors.New("treatment not provided"), "--treatment required")
	}

	if !*confirm {
		fmt.Printf("Delete all entries for treatment=%s? [y/N] ", *treatment)
		var response string
		if _, err := fmt.Scanln(&response); err != nil && !stdErrors.Is(err, io.EOF) {
			return cgerr.NewInternalError(errors.Wrap(err, "read confirmation"), "failed to read confirmation input")
		}
		trimmed := strings.TrimSpace(response)
		if trimmed != "y" && trimmed != "Y" {
			fmt.Println("cancelled")
			return nil
		}
	}

	db, err := openDB(*dbPath)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := db.Close(); cerr != nil {
			logx.Warn(ctx, "failed to close database", cgerr.NewInternalError(errors.Wrap(cerr, "close database"), "failed to close database").WithDetail("path", *dbPath))
		}
	}()

	res, err := db.Exec("DELETE FROM queue WHERE treatment=?", *treatment)
	if err != nil {
		return cgerr.NewInternalError(errors.Wrap(err, "delete queue entries"), "failed to reset treatment").WithDetail("treatment", *treatment)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return cgerr.NewInternalError(errors.Wrap(err, "read rows affected"), "failed to read deletion count")
	}
	fmt.Printf("deleted %d entries\n", n)

	return nil
}
