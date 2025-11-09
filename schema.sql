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
