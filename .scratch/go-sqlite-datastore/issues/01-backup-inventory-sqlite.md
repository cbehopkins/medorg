# 01: Move Backup Inventory and Priority Planning to SQLite

**What to build:** The backup workflow plans and performs backups using bounded SQLite-backed inventory and candidate iteration, while retaining the current file selection and metadata behavior.

**Blocked by:** None (can start immediately).

**Status:** ready-for-agent

- [x] Source and destination content are grouped by checksum, repeated-content destination metadata is merged, and the current source-path selection behavior is preserved.
- [x] Content already on the destination or exceeding the configured backup limit is excluded using the current production boundary behavior.
- [x] Eligible content is selected by fewest existing destinations, then largest file, then deterministic path order.
- [x] Destination-only content remains available to orphan reporting or deletion, according to the existing mode.
- [x] If a large file cannot fit on the destination, the workflow can continue to smaller eligible files where current behavior permits.
- [x] Only successful copies update source and destination `.medorg.xml` metadata and the in-run destination inventory.
- [x] Large query results are iterated without materialising the full inventory or candidate set in Go memory.
- [x] The temporary backup database and its SQLite sidecar files are removed when the backup run closes, including error paths.
- [x] Workflow-level tests verify resulting file contents, metadata, ordering where observable, orphan behavior, and destination-full fallback without asserting SQLite schema or query internals.
