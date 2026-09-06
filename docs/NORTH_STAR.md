# north star — next

(mirror of ~/Projects/kg/Project/next/NORTH_STAR.md — dk edits that file; this copy is generated.)

★ A file-by-file job survives any interruption and resumes exactly where it stopped.

*Written 2026-09-06 (sd-mzgy.9). dk edits this file; nothing else is a source. The repo's `docs/NORTH_STAR.md` and `docs/ROADMAP.md` mirror the ★ line.*

next is a deterministic, resumable job queue for processing many files one at a time: pipe-friendly, SQLite-backed, zero dependencies. It exists so a long run over thousands of files can be killed, moved, or crashed and picked up at the next file rather than the first.
