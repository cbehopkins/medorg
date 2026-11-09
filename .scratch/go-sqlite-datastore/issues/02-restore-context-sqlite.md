# 02: Move Restore-Context State to SQLite

**What to build:** A restore context can ingest a journal, resume across separate `mdrestore` commands and backup volumes, and retain correct pending/copied state in SQLite.

**Blocked by:** None (can start immediately).

**Status:** ready-for-agent

- [ ] Journal ingestion groups work by the existing content identity while retaining every distinct restore target and its required backup destinations.
- [ ] Pending and copied state persists after closing and reopening the database between `newdb` and `copy` invocations.
- [ ] Copying a target moves only that target from pending to copied; other targets for shared content remain pending.
- [ ] The pending-to-copied database transition is atomic and occurs only after the file copy succeeds.
- [ ] Pending work remains reportable by required backup volume, and repeated copy invocations remain idempotent.
- [ ] Opening an existing SQLite restore database continues the active restore context; a new context can use a new database path. Existing bobbob databases are not silently treated as SQLite or migrated.
- [ ] The Go restore store uses a pure-Go SQLite driver and remains independent of the Python database schema.
- [ ] Workflow-level tests exercise journal ingestion and copy across reopened database sessions, checking outcomes through the restore behavior rather than SQLite internals.
