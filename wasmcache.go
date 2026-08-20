package main

// SQLite here is not a C library — it is a WebAssembly module (ncruces/go-sqlite3
// + its `embed` package) that wazero compiles to native code. That compile is
// the whole reason this file exists.
//
// Without a compilation cache, wazero re-JITs the entire SQLite module in EVERY
// `next` process, before the first query and before the database file is even
// opened. Measured on an M-series arm64 machine, 2026-08-20: ~1.7s of CPU per
// invocation, paid at connect time (`next --version`, which opens no database,
// costs 0.37s; `next status --db=...` costs 1.7s CPU — that delta is the JIT).
//
// `next` is a per-unit CLI: callers run it in a loop, one process per unit.
// sdlc's sweep-queue-test.sh execs it 52 times, so the toll there is ~88s of
// pure CPU before any real work. On a loaded machine that wall-clock cost
// multiplies — the suite blew a 180s gate cap at load 59 on 8 cores while
// passing comfortably at load 21 (sdlc bead sd-3ep).
//
// wazero's compilation cache stores the compiled machine code on disk and
// mmaps it on subsequent runs, so the JIT is paid once per (binary, wazero
// version) instead of once per process.

import (
	"math/bits"
	"os"
	"path/filepath"
	"sync"

	"github.com/ncruces/go-sqlite3"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

// wasmCacheOnce guards the one-time install of the shared wazero runtime config.
// It must run before the first database connection, because ncruces/go-sqlite3
// reads sqlite3.RuntimeConfig inside a sync.Once of its own (compileSQLite) and
// never looks at it again.
var wasmCacheOnce sync.Once

// initWasmCache points ncruces/go-sqlite3 at a persistent wazero compilation
// cache. It is best-effort: if the cache directory cannot be created, the
// runtime config is left nil and the library falls back to its own defaults —
// slower, but correct.
func initWasmCache() {
	wasmCacheOnce.Do(func() {
		root, err := os.UserCacheDir()
		if err != nil {
			return
		}
		dir := filepath.Join(root, "next", "wazero")
		if mkErr := os.MkdirAll(dir, 0o750); mkErr != nil {
			return
		}
		cache, err := wazero.NewCompilationCacheWithDir(dir)
		if err != nil {
			return
		}

		// Setting RuntimeConfig REPLACES the library's own defaults wholesale
		// (sqlite.go compileSQLite: `cfg := RuntimeConfig; if cfg == nil { ... }`),
		// so the memory limit and core-feature set below must mirror upstream
		// exactly. Changing them here would silently change SQLite's behaviour,
		// which is not what this fix is for.
		pages := uint32(4096) // 256MB on 64-bit, matching upstream
		if bits.UintSize < 64 {
			pages = 512 // 32MB on 32-bit, matching upstream
		}
		sqlite3.RuntimeConfig = wazero.NewRuntimeConfig().
			WithMemoryLimitPages(pages).
			WithCoreFeatures(api.CoreFeaturesV2).
			WithCompilationCache(cache)
	})
}
