-- next: simplified job ledger schema
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS queue (
  path TEXT NOT NULL,
  path_hash TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  treatment TEXT NOT NULL,
  done_at TEXT,
  result TEXT,
  next_at TEXT,
  PRIMARY KEY (path, treatment)
);

CREATE INDEX IF NOT EXISTS idx_pending ON queue(treatment, path_hash) 
  WHERE done_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_revisit ON queue(treatment, next_at)
  WHERE next_at IS NOT NULL;
