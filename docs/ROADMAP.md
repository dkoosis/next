# next

★ A file-by-file job survives any interruption and resumes exactly where it stopped.

(mirrors `docs/NORTH_STAR.md`, which owns the line — dk edits that file and nothing
else is a source.)

next is a deterministic, resumable job queue for processing many files one at a time: pipe-friendly, SQLite-backed, zero dependencies. It exists so a long run over thousands of files can be killed, moved, or crashed and picked up at the next file rather than the first.

## Epics

_none filed yet — the repo has no epics; work is tracked as tasks in bd_
