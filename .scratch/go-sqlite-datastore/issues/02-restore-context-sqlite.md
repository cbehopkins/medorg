# 02: Move Restore-Context State to SQLite

**What to build:** A restore context can ingest a journal, resume across separate `mdrestore` commands and backup volumes, and retain correct pending/copied state in SQLite.

**Blocked by:** None (can start immediately).

**Status:** resolved

- [x] Journal ingestion groups work by the existing content identity while retaining every distinct restore target and its required backup destinations.
- [x] Pending and copied state persists after closing and reopening the database between `newdb` and `copy` invocations.
- [x] Copying a target moves only that target from pending to copied; other targets for shared content remain pending.
- [x] The pending-to-copied database transition is atomic and occurs only after the file copy succeeds.
- [x] Pending work remains reportable by required backup volume, and repeated copy invocations remain idempotent.
- [x] Opening an existing SQLite restore database continues the active restore context; a new context can use a new database path. Existing bobbob databases are not silently treated as SQLite or migrated.
- [x] The Go restore store uses a pure-Go SQLite driver and remains independent of the Python database schema.
- [x] Workflow-level tests exercise journal ingestion and copy across reopened database sessions, checking outcomes through the restore behavior rather than SQLite internals.

## Comments

Replaced bobbob-backed restore state with a pure-Go SQLite store. Restore targets now transition independently in a transaction after successful copies, repeated ingestion preserves copied state, and non-SQLite legacy files are rejected without modification. `go test ./pkg/consumers ./cmd/mdrestore -count=1` passed.
