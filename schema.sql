CREATE TABLE IF NOT EXISTS queue (
  path          TEXT NOT NULL,
  path_hash     TEXT NOT NULL,
  content_hash  TEXT NOT NULL,
  spec_hash     TEXT,
  treatment     TEXT NOT NULL,
  done_at       TEXT,
  result        TEXT,
  next_at       TEXT,
  claimed_at    TEXT,
  claimed_by    TEXT,
  updated_at    TEXT,
  PRIMARY KEY (path_hash, treatment)
);
CREATE INDEX IF NOT EXISTS idx_queue_treatment_done ON queue(treatment, done_at);
CREATE INDEX IF NOT EXISTS idx_queue_next_at ON queue(next_at);
-- idx_queue_treatment_updated is created in Go (openDB) after migration, not
-- here: on an old on-disk DB this CREATE TABLE IF NOT EXISTS is a no-op, so
-- an index referencing updated_at here would fail before the ALTER TABLE
-- migration adds that column.
