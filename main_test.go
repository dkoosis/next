package main

import (
	"bytes"
	"context"
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

//nolint:dupword // SQL NULL repetition is intentional
const testInsertSQL = `
	INSERT INTO queue (path, path_hash, content_hash, treatment, done_at, result, next_at)
	VALUES (?, ?, ?, ?, NULL, NULL, NULL)
`

func setupWorkDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Chdir(dir)
	return dir
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w

	var buf bytes.Buffer
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(&buf, r)
		close(done)
	}()

	fn()

	_ = w.Close()
	os.Stdout = old
	<-done
	_ = r.Close()

	return strings.TrimSpace(buf.String())
}

func withCLI(t *testing.T, args []string, stdin io.Reader, fn func()) string {
	t.Helper()

	oldArgs := os.Args
	os.Args = args
	defer func() { os.Args = oldArgs }()

	if stdin == nil {
		return captureStdout(t, fn)
	}

	oldStdin := os.Stdin
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	if _, err := io.Copy(w, stdin); err != nil {
		t.Fatalf("copy stdin: %v", err)
	}
	_ = w.Close()
	os.Stdin = r
	defer func() {
		os.Stdin = oldStdin
		_ = r.Close()
	}()

	return captureStdout(t, fn)
}

func withStdin(t *testing.T, input string, fn func()) {
	t.Helper()

	oldStdin := os.Stdin
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	if _, err := io.WriteString(w, input); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	_ = w.Close()
	os.Stdin = r
	defer func() {
		os.Stdin = oldStdin
		_ = r.Close()
	}()

	fn()
}

func TestOpenDB_CreatesUsableQueueSchema(t *testing.T) {
	tmpDir := setupWorkDir(t)
	dbPath := filepath.Join(tmpDir, "ledger.db")

	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(testInsertSQL, "/tmp/f", pathHash("/tmp/f"), "ch", "lint"); err != nil {
		t.Fatalf("insert queue row: %v", err)
	}

	var claimedAt, claimedBy sql.NullString
	if err := db.QueryRow("SELECT claimed_at, claimed_by FROM queue WHERE treatment='lint'").Scan(&claimedAt, &claimedBy); err != nil {
		t.Fatalf("query claim columns: %v", err)
	}
}

func TestEnqueueCmd_QueuesSupportedInputs(t *testing.T) {
	tmpDir := setupWorkDir(t)
	dbPath := filepath.Join(tmpDir, "ledger.db")

	filePath := filepath.Join(tmpDir, "a.txt")
	if err := os.WriteFile(filePath, []byte("hello"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	dirPath := filepath.Join(tmpDir, "pkg")
	if err := os.MkdirAll(dirPath, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dirPath, "x.go"), []byte("package pkg"), 0o600); err != nil {
		t.Fatalf("write dir file: %v", err)
	}

	input := strings.NewReader(strings.Join([]string{
		filepath.Join(tmpDir, "missing.txt"),
		filePath,
		dirPath,
		"",
	}, "\n"))

	out := withCLI(t, []string{"next", "enqueue", "--db", dbPath, "--treatment", "lint"}, input, enqueueCmd)
	if out != "enqueued 2 paths for treatment=lint" {
		t.Fatalf("unexpected output: %q", out)
	}

	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB verify: %v", err)
	}
	defer func() { _ = db.Close() }()

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM queue WHERE treatment='lint'").Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != 2 {
		t.Fatalf("queued row count = %d, want 2", count)
	}
}

func TestEnqueueFromStdin_ReactivatesDoneEntryOnContentChange(t *testing.T) {
	tmpDir := setupWorkDir(t)
	dbPath := filepath.Join(tmpDir, "ledger.db")
	pkgDir := filepath.Join(tmpDir, "pkg")
	if err := os.MkdirAll(pkgDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(pkgDir, "a.go"), []byte("v1"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	absDir, _ := filepath.Abs(pkgDir)
	withStdin(t, absDir+"\n", func() {
		if _, err := enqueueFromStdin(db, "lint"); err != nil {
			t.Fatalf("enqueue initial: %v", err)
		}
	})

	if _, err := db.Exec("UPDATE queue SET done_at=?, result='ok' WHERE path_hash=?", time.Now().UTC().Format(time.RFC3339), pathHash(absDir)); err != nil {
		t.Fatalf("mark done: %v", err)
	}
	if err := os.WriteFile(filepath.Join(pkgDir, "b.go"), []byte("v2"), 0o600); err != nil {
		t.Fatalf("write changed file: %v", err)
	}

	withStdin(t, absDir+"\n", func() {
		if _, err := enqueueFromStdin(db, "lint"); err != nil {
			t.Fatalf("enqueue changed: %v", err)
		}
	})

	var doneAt sql.NullString
	if err := db.QueryRow("SELECT done_at FROM queue WHERE path_hash=?", pathHash(absDir)).Scan(&doneAt); err != nil {
		t.Fatalf("read done_at: %v", err)
	}
	if doneAt.Valid {
		t.Fatal("done_at should be cleared when content hash changes")
	}
}

func TestClaimCmd_ClaimsOnlyEligibleRows(t *testing.T) {
	tmpDir := setupWorkDir(t)
	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	mk := func(name string) string {
		p := filepath.Join(tmpDir, name)
		if err := os.WriteFile(p, []byte(name), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		return p
	}
	pending := mk("pending.txt")
	due := mk("due.txt")
	future := mk("future.txt")
	claimed := mk("claimed.txt")

	_, _ = db.Exec(testInsertSQL, pending, pathHash(pending), "h1", "lint")
	past := time.Now().Add(-time.Hour).UTC().Format("2006-01-02 15:04:05")
	_, _ = db.Exec(`INSERT INTO queue (path,path_hash,content_hash,treatment,done_at,result,next_at) VALUES (?,?,?,?,?,?,?)`, due, pathHash(due), "h2", "lint", past, "old", past)
	futureTime := time.Now().Add(time.Hour).UTC().Format("2006-01-02 15:04:05")
	_, _ = db.Exec(`INSERT INTO queue (path,path_hash,content_hash,treatment,done_at,result,next_at) VALUES (?,?,?,?,?,?,?)`, future, pathHash(future), "h3", "lint", past, "old", futureTime)
	_, _ = db.Exec(`INSERT INTO queue (path,path_hash,content_hash,treatment,done_at,result,next_at,claimed_at,claimed_by) VALUES (?,?,?,?,?,?,?,?,?)`, claimed, pathHash(claimed), "h4", "lint", nil, nil, nil, time.Now().UTC().Format("2006-01-02 15:04:05"), "w0")
	_ = db.Close()

	out := withCLI(t, []string{"next", "claim", "--db", dbPath, "--treatment", "lint", "--n", "10", "--worker", "w1"}, nil, claimCmd)
	got := strings.Fields(out)
	sort.Strings(got)
	want := []string{due, pending}
	sort.Strings(want)
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("claimed paths = %v, want %v", got, want)
	}

	db, err = openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB verify: %v", err)
	}
	defer func() { _ = db.Close() }()
	var claimedBy string
	if err := db.QueryRow("SELECT claimed_by FROM queue WHERE path=?", pending).Scan(&claimedBy); err != nil {
		t.Fatalf("query claimed_by: %v", err)
	}
	if claimedBy != "w1" {
		t.Fatalf("claimed_by = %q, want w1", claimedBy)
	}
}

func TestClaimCmd_ShardsPartitionResults(t *testing.T) {
	tmpDir := setupWorkDir(t)
	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	for i := range 48 {
		p := filepath.Join(tmpDir, fmt.Sprintf("f-%03d.txt", i))
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		if _, err := db.Exec(testInsertSQL, p, pathHash(p), fmt.Sprintf("ch-%d", i), "lint"); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	_ = db.Close()

	const totalShards = 4
	all := map[string]bool{}
	for shard := range totalShards {
		out := withCLI(t, []string{"next", "claim", "--db", dbPath, "--treatment", "lint", "--shard", fmt.Sprint(shard), "--total-shards", fmt.Sprint(totalShards), "--n", "1000", "--worker", fmt.Sprintf("w%d", shard)}, nil, claimCmd)
		for _, p := range strings.Fields(out) {
			if all[p] {
				t.Fatalf("duplicate claimed across shards: %s", p)
			}
			all[p] = true
		}
	}
	if len(all) != 48 {
		t.Fatalf("total unique claims = %d, want 48", len(all))
	}
}

func TestDoneStatusAndResetWorkflow(t *testing.T) {
	tmpDir := setupWorkDir(t)
	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	p1 := filepath.Join(tmpDir, "a.txt")
	p2 := filepath.Join(tmpDir, "b.txt")
	_ = os.WriteFile(p1, []byte("a"), 0o600)
	_ = os.WriteFile(p2, []byte("b"), 0o600)
	_, _ = db.Exec(testInsertSQL, p1, pathHash(p1), "h1", "lint")
	_, _ = db.Exec(testInsertSQL, p2, pathHash(p2), "h2", "other")
	_ = db.Close()

	_ = withCLI(t, []string{"next", "done", "--db", dbPath, "--treatment", "lint", "--path", p1, "--result", "ok", "--revisit", "+14 days"}, nil, doneCmd)
	status := withCLI(t, []string{"next", "status", "--db", dbPath, "--treatment", "lint"}, nil, statusCmd)
	if !strings.Contains(status, "lint") || !strings.Contains(status, "0") || !strings.Contains(status, "1") {
		t.Fatalf("unexpected status output: %q", status)
	}

	resetOut := withCLI(t, []string{"next", "reset", "--db", dbPath, "--treatment", "lint", "--yes"}, nil, resetCmd)
	if resetOut != "deleted 1 entries" {
		t.Fatalf("unexpected reset output: %q", resetOut)
	}

	db, err = openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB verify: %v", err)
	}
	defer func() { _ = db.Close() }()

	var lintCount, otherCount int
	_ = db.QueryRow("SELECT COUNT(*) FROM queue WHERE treatment='lint'").Scan(&lintCount)
	_ = db.QueryRow("SELECT COUNT(*) FROM queue WHERE treatment='other'").Scan(&otherCount)
	if lintCount != 0 || otherCount != 1 {
		t.Fatalf("counts after reset: lint=%d other=%d", lintCount, otherCount)
	}
}

func TestValidationHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		input  string
		valid  bool
		lease  bool
	}{
		{name: "minutes", input: "5 minutes", valid: true, lease: true},
		{name: "plus days", input: "+14 days", valid: true, lease: true},
		{name: "negative valid", input: "-7 days", valid: true, lease: false},
		{name: "invalid word", input: "never", valid: false, lease: false},
		{name: "zero", input: "0 days", valid: false, lease: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := validTimeModifier(tc.input); got != tc.valid {
				t.Fatalf("validTimeModifier(%q) = %v, want %v", tc.input, got, tc.valid)
			}
			if got := validLease(tc.input); got != tc.lease {
				t.Fatalf("validLease(%q) = %v, want %v", tc.input, got, tc.lease)
			}
		})
	}
}

func TestCalculateShardRange_IsContiguousAndCoversSpace(t *testing.T) {
	t.Parallel()

	for _, total := range []int{2, 4, 8} {
		t.Run(fmt.Sprintf("total=%d", total), func(t *testing.T) {
			starts := make([]string, total)
			ends := make([]string, total)
			for i := range total {
				starts[i], ends[i] = calculateShardRange(i, total)
				if starts[i] >= ends[i] {
					t.Fatalf("invalid range shard %d: [%s,%s)", i, starts[i], ends[i])
				}
			}
			if starts[0] != strings.Repeat("0", 64) {
				t.Fatalf("first shard start = %s", starts[0])
			}
			for i := 0; i < total-1; i++ {
				if ends[i] != starts[i+1] {
					t.Fatalf("non-contiguous between shard %d and %d", i, i+1)
				}
			}
			if !strings.HasPrefix(ends[total-1], "ffffffffffffffff") {
				t.Fatalf("last shard end = %s", ends[total-1])
			}
		})
	}
}

func FuzzValidTimeModifier(f *testing.F) {
	for _, seed := range []string{"5 minutes", "+14 days", "-3 hours", "", "random"} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, s string) {
		_ = validTimeModifier(s)
		_ = validLease(s)
	})
}

func TestMarkDone_ReturnsErrorForMissingEntry(t *testing.T) {
	tmpDir := setupWorkDir(t)
	db, err := openDB(filepath.Join(tmpDir, "ledger.db"))
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	err = markDone(db, pathHash("/not-found"), "lint", "", "")
	if err == nil || !strings.Contains(err.Error(), ErrNoQueueEntry.Error()) {
		t.Fatalf("expected ErrNoQueueEntry, got %v", err)
	}
}

func TestFileAndDirHash_BasicBehavior(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	fp := filepath.Join(tmpDir, "a.txt")
	if err := os.WriteFile(fp, []byte("hello world\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	h, err := fileHash(fp)
	if err != nil || len(h) != 64 {
		t.Fatalf("fileHash err=%v hash=%q", err, h)
	}
	if _, err := fileHash(filepath.Join(tmpDir, "missing")); err == nil {
		t.Fatal("expected error for missing file")
	}

	dh1, err := dirHash(tmpDir)
	if err != nil {
		t.Fatalf("dirHash: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "b.txt"), []byte("b"), 0o600); err != nil {
		t.Fatalf("write b: %v", err)
	}
	dh2, err := dirHash(tmpDir)
	if err != nil {
		t.Fatalf("dirHash 2: %v", err)
	}
	if dh1 == dh2 {
		t.Fatal("dir hash should change when files change")
	}
}

func TestStatusCmd_JSONOutput(t *testing.T) {
	tmpDir := setupWorkDir(t)
	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	_, _ = db.Exec(testInsertSQL, "/a", pathHash("/a"), "h", "lint")
	_, _ = db.Exec(`INSERT INTO queue (path, path_hash, content_hash, treatment, done_at, result, next_at) VALUES (?, ?, ?, ?, ?, ?, NULL)`, "/b", pathHash("/b"), "h2", "lint", time.Now().UTC().Format(time.RFC3339), "ok")
	_ = db.Close()

	out := withCLI(t, []string{"next", "status", "--db", dbPath, "--json"}, nil, statusCmd)
	if !strings.Contains(out, `"treatment": "lint"`) || !strings.Contains(out, `"treatment": "TOTAL"`) {
		t.Fatalf("unexpected json output: %s", out)
	}
}

func TestMarkDone_SetsNextAtWhenRevisitProvided(t *testing.T) {
	tmpDir := setupWorkDir(t)
	db, err := openDB(filepath.Join(tmpDir, "ledger.db"))
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	path := "/needs-revisit"
	if _, err := db.Exec(testInsertSQL, path, pathHash(path), "h", "lint"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := markDone(db, pathHash(path), "lint", "ok", "+14 days"); err != nil {
		t.Fatalf("markDone: %v", err)
	}

	var nextAt sql.NullString
	if err := db.QueryRowContext(context.Background(), "SELECT next_at FROM queue WHERE path_hash=?", pathHash(path)).Scan(&nextAt); err != nil {
		t.Fatalf("query next_at: %v", err)
	}
	if !nextAt.Valid {
		t.Fatal("expected next_at to be set")
	}
}
