package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

func setupWorkDir(t *testing.T, withSchema bool) (string, func()) {
	t.Helper()
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}

	tmpDir := t.TempDir()
	if withSchema {
		qualityDir := filepath.Join(tmpDir, ".quality")
		if err := os.MkdirAll(qualityDir, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		schema := `CREATE TABLE IF NOT EXISTS queue (
    path TEXT PRIMARY KEY,
    path_hash TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    treatment TEXT NOT NULL,
    done_at TEXT,
    result TEXT,
    next_at TEXT
);`
		if err := os.WriteFile(filepath.Join(qualityDir, "schema.sql"), []byte(schema), 0o600); err != nil {
			t.Fatalf("write schema: %v", err)
		}
	}

	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	return tmpDir, func() {
		if err := os.Chdir(oldWD); err != nil {
			t.Fatalf("restore chdir: %v", err)
		}
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w

	var buf bytes.Buffer
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(&buf, r)
		close(done)
	}()

	fn()

	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	os.Stdout = oldStdout
	if err := r.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}
	<-done

	return buf.String()
}

func TestFileHash_ReturnsDigest_When_FileReadable(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "sample.txt")
	content := []byte("hello world\n")
	if err := os.WriteFile(filePath, content, 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	got, err := fileHash(filePath)
	if err != nil {
		t.Fatalf("fileHash error: %v", err)
	}

	// Known SHA-256 digest for "hello world\n"
	const want = "a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447"
	if got != want {
		t.Fatalf("fileHash = %s, want %s", got, want)
	}
}

func TestFileHash_ReturnsError_When_FileMissing(t *testing.T) {
	t.Parallel()

	if _, err := fileHash("/non-existent-file" + time.Now().Format("150405")); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestOpenDB_CreatesSchema_When_SchemaPresent(t *testing.T) {
	tmpDir, restore := setupWorkDir(t, true)
	defer restore()

	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	var journalMode string
	if err := db.QueryRow("PRAGMA journal_mode;").Scan(&journalMode); err != nil {
		t.Fatalf("query journal_mode: %v", err)
	}
	if strings.ToLower(journalMode) != "wal" {
		t.Fatalf("journal mode = %s, want wal", journalMode)
	}

	if _, err := db.Exec(`
        INSERT INTO queue (path, path_hash, content_hash, treatment, done_at, result, next_at)
        VALUES (?, ?, ?, ?, NULL, NULL, NULL)
    `, "/tmp/file", pathHash("/tmp/file"), "hash", "lint"); err != nil {
		t.Fatalf("insert queue: %v", err)
	}
}

func TestOpenDB_ReturnsDB_When_SchemaMissing(t *testing.T) {
	tmpDir, restore := setupWorkDir(t, false)
	defer restore()

	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

func TestEnqueueCmd_InsertsRows_When_InputContainsValidPaths(t *testing.T) {
	tmpDir, restore := setupWorkDir(t, true)
	defer restore()

	dbPath := filepath.Join(tmpDir, "ledger.db")

	validFile := filepath.Join(tmpDir, "valid.txt")
	if err := os.WriteFile(validFile, []byte("data"), 0o600); err != nil {
		t.Fatalf("write valid file: %v", err)
	}
	validAbs, err := filepath.Abs(validFile)
	if err != nil {
		t.Fatalf("abs: %v", err)
	}

	inputFile, err := os.CreateTemp(tmpDir, "input")
	if err != nil {
		t.Fatalf("create temp input: %v", err)
	}
	defer inputFile.Close()

	fmt.Fprintln(inputFile, "")
	fmt.Fprintln(inputFile, filepath.Join(tmpDir, "missing.txt"))
	fmt.Fprintln(inputFile, validAbs)
	fmt.Fprintln(inputFile, validAbs)
	if _, err := inputFile.Seek(0, 0); err != nil {
		t.Fatalf("seek: %v", err)
	}

	oldArgs := os.Args
	os.Args = []string{"next", "enqueue", "--db", dbPath, "--treatment", "lint"}
	defer func() { os.Args = oldArgs }()

	oldStdin := os.Stdin
	os.Stdin = inputFile
	defer func() { os.Stdin = oldStdin }()

	output := captureStdout(t, func() {
		enqueueCmd()
	})

	// Duplicate entries still increment the reported count because INSERT OR IGNORE
	// does not return an error for ignored rows.
	if !strings.Contains(output, "enqueued 2 paths for treatment=lint") {
		t.Fatalf("unexpected output: %q", output)
	}

	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB verify: %v", err)
	}
	defer db.Close()

	row := db.QueryRow("SELECT path, content_hash FROM queue WHERE treatment=?", "lint")
	var storedPath, storedHash string
	if err := row.Scan(&storedPath, &storedHash); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if storedPath != validAbs {
		t.Fatalf("stored path = %s, want %s", storedPath, validAbs)
	}
	wantHash, err := fileHash(validAbs)
	if err != nil {
		t.Fatalf("fileHash: %v", err)
	}
	if storedHash != wantHash {
		t.Fatalf("stored hash = %s, want %s", storedHash, wantHash)
	}

	countRow := db.QueryRow("SELECT COUNT(*) FROM queue")
	var count int
	if err := countRow.Scan(&count); err != nil {
		t.Fatalf("count scan: %v", err)
	}
	if count != 1 {
		t.Fatalf("queue count = %d, want 1", count)
	}
}

func TestClaimCmd_PrintsPendingPaths_When_CursorSpecified(t *testing.T) {
	tmpDir, restore := setupWorkDir(t, true)
	defer restore()

	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	paths := []string{
		filepath.Join(tmpDir, "a.txt"),
		filepath.Join(tmpDir, "b.txt"),
		filepath.Join(tmpDir, "c.txt"),
	}
	type candidate struct {
		path string
		hash string
	}
	candidates := make([]candidate, 0, len(paths))
	for i, p := range paths {
		if err := os.WriteFile(p, []byte(fmt.Sprintf("file-%d", i)), 0o600); err != nil {
			t.Fatalf("write file %s: %v", p, err)
		}
		if _, err := db.Exec(`
            INSERT INTO queue (path, path_hash, content_hash, treatment, done_at, result, next_at)
        VALUES (?, ?, ?, ?, NULL, NULL, NULL)
        `, p, pathHash(p), fmt.Sprintf("hash-%d", i), "lint"); err != nil {
			t.Fatalf("insert %s: %v", p, err)
		}
		candidates = append(candidates, candidate{path: p, hash: pathHash(p)})
	}

	donePath := filepath.Join(tmpDir, "done.txt")
	if err := os.WriteFile(donePath, []byte("done"), 0o600); err != nil {
		t.Fatalf("write done file: %v", err)
	}
	if _, err := db.Exec(`
        INSERT INTO queue (path, path_hash, content_hash, treatment, done_at, result, next_at)
        VALUES (?, ?, ?, ?, ?, ?, NULL)
    `, donePath, pathHash(donePath), "hash-done", "lint", time.Now().UTC().Format(time.RFC3339), "ok"); err != nil {
		t.Fatalf("insert done: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].hash < candidates[j].hash
	})
	if len(candidates) < 3 {
		t.Fatalf("need at least 3 candidates, got %d", len(candidates))
	}
	cursor := candidates[0].hash
	expected := []string{candidates[1].path, candidates[2].path}
	oldArgs := os.Args
	os.Args = []string{"next", "claim", "--db", dbPath, "--treatment", "lint", "--cursor", cursor, "--n", "2"}
	defer func() { os.Args = oldArgs }()

	output := captureStdout(t, func() {
		claimCmd()
	})

	lines := strings.Split(strings.TrimSpace(output), "\n")
	var results []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			results = append(results, trimmed)
		}
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d (%q)", len(results), results)
	}
	if results[0] != expected[0] {
		t.Fatalf("first result = %s, want %s", results[0], expected[0])
	}
	if results[1] != expected[1] {
		t.Fatalf("second result = %s, want %s", results[1], expected[1])
	}
}

func TestStatusCmd_ShowsCounts_When_FilteredByTreatment(t *testing.T) {
	tmpDir, restore := setupWorkDir(t, true)
	defer restore()

	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	pendingPath := filepath.Join(tmpDir, "pending.txt")
	if err := os.WriteFile(pendingPath, []byte("pending"), 0o600); err != nil {
		t.Fatalf("write pending: %v", err)
	}
	if _, err := db.Exec(`
        INSERT INTO queue (path, path_hash, content_hash, treatment, done_at, result, next_at)
        VALUES (?, ?, ?, ?, NULL, NULL, NULL)
    `, pendingPath, pathHash(pendingPath), "hash-pending", "lint"); err != nil {
		t.Fatalf("insert pending: %v", err)
	}

	donePath := filepath.Join(tmpDir, "done.txt")
	if err := os.WriteFile(donePath, []byte("done"), 0o600); err != nil {
		t.Fatalf("write done: %v", err)
	}
	if _, err := db.Exec(`
        INSERT INTO queue (path, path_hash, content_hash, treatment, done_at, result, next_at)
        VALUES (?, ?, ?, ?, ?, ?, NULL)
    `, donePath, pathHash(donePath), "hash-done", "lint", time.Now().UTC().Format(time.RFC3339), "ok"); err != nil {
		t.Fatalf("insert done: %v", err)
	}

	otherPath := filepath.Join(tmpDir, "other.txt")
	if err := os.WriteFile(otherPath, []byte("other"), 0o600); err != nil {
		t.Fatalf("write other: %v", err)
	}
	if _, err := db.Exec(`
        INSERT INTO queue (path, path_hash, content_hash, treatment, done_at, result, next_at)
        VALUES (?, ?, ?, ?, NULL, NULL, NULL)
    `, otherPath, pathHash(otherPath), "hash-other", "other"); err != nil {
		t.Fatalf("insert other: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	oldArgs := os.Args
	os.Args = []string{"next", "status", "--db", dbPath, "--treatment", "lint"}
	defer func() { os.Args = oldArgs }()

	output := captureStdout(t, func() {
		statusCmd()
	})

	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected header + 1 line, got %d", len(lines))
	}

	fields := strings.Fields(lines[1])
	if len(fields) != 3 {
		t.Fatalf("unexpected fields: %v", fields)
	}
	if fields[0] != "lint" {
		t.Fatalf("treatment field = %s, want lint", fields[0])
	}
	if fields[1] != "1" {
		t.Fatalf("pending = %s, want 1", fields[1])
	}
	if fields[2] != "1" {
		t.Fatalf("done = %s, want 1", fields[2])
	}
}

func TestResetCmd_DeletesEntries_When_Confirmed(t *testing.T) {
	tmpDir, restore := setupWorkDir(t, true)
	defer restore()

	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	lintPath := filepath.Join(tmpDir, "lint.txt")
	if err := os.WriteFile(lintPath, []byte("lint"), 0o600); err != nil {
		t.Fatalf("write lint: %v", err)
	}
	if _, err := db.Exec(`
        INSERT INTO queue (path, path_hash, content_hash, treatment, done_at, result, next_at)
        VALUES (?, ?, ?, ?, NULL, NULL, NULL)
    `, lintPath, pathHash(lintPath), "hash-lint", "lint"); err != nil {
		t.Fatalf("insert lint: %v", err)
	}

	otherPath := filepath.Join(tmpDir, "other.txt")
	if err := os.WriteFile(otherPath, []byte("other"), 0o600); err != nil {
		t.Fatalf("write other: %v", err)
	}
	if _, err := db.Exec(`
        INSERT INTO queue (path, path_hash, content_hash, treatment, done_at, result, next_at)
        VALUES (?, ?, ?, ?, NULL, NULL, NULL)
    `, otherPath, pathHash(otherPath), "hash-other", "other"); err != nil {
		t.Fatalf("insert other: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	oldArgs := os.Args
	os.Args = []string{"next", "reset", "--db", dbPath, "--treatment", "lint", "--yes"}
	defer func() { os.Args = oldArgs }()

	output := captureStdout(t, func() {
		resetCmd()
	})

	if !strings.Contains(output, "deleted 1 entries") {
		t.Fatalf("unexpected output: %q", output)
	}

	dbVerify, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer dbVerify.Close()

	var lintCount int
	if err := dbVerify.QueryRow("SELECT COUNT(*) FROM queue WHERE treatment=?", "lint").Scan(&lintCount); err != nil {
		t.Fatalf("lint count: %v", err)
	}
	if lintCount != 0 {
		t.Fatalf("lint count = %d, want 0", lintCount)
	}

	var otherCount int
	if err := dbVerify.QueryRow("SELECT COUNT(*) FROM queue WHERE treatment=?", "other").Scan(&otherCount); err != nil {
		t.Fatalf("other count: %v", err)
	}
	if otherCount != 1 {
		t.Fatalf("other count = %d, want 1", otherCount)
	}
}

func TestDoneCmd_MarksEntryDone_When_PathProvided(t *testing.T) {
	tmpDir, restore := setupWorkDir(t, true)
	defer restore()

	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	targetPath := filepath.Join(tmpDir, "target.txt")
	if err := os.WriteFile(targetPath, []byte("target"), 0o600); err != nil {
		t.Fatalf("write target: %v", err)
	}
	absTarget, err := filepath.Abs(targetPath)
	if err != nil {
		t.Fatalf("abs target: %v", err)
	}

	if _, err := db.Exec(`
        INSERT INTO queue (path, path_hash, content_hash, treatment, done_at, result, next_at)
        VALUES (?, ?, ?, ?, NULL, NULL, NULL)
    `, absTarget, pathHash(absTarget), "hash-target", "lint"); err != nil {
		t.Fatalf("insert target: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	oldArgs := os.Args
	os.Args = []string{"next", "done", "--db", dbPath, "--path", absTarget, "--treatment", "lint", "--result", "abc123"}
	defer func() { os.Args = oldArgs }()

	doneCmd()

	verifyDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer verifyDB.Close()

	row := verifyDB.QueryRow("SELECT done_at, result, next_at FROM queue WHERE path=?", absTarget)
	var doneAt, result, nextAt sql.NullString
	if err := row.Scan(&doneAt, &result, &nextAt); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !doneAt.Valid {
		t.Fatal("done_at not set")
	}
	if !result.Valid || result.String != "abc123" {
		t.Fatalf("result = %v, want abc123", result)
	}
	if nextAt.Valid {
		t.Fatalf("expected next_at NULL, got %v", nextAt)
	}
}
