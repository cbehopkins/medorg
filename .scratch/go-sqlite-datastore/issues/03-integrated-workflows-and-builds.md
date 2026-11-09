# 03: Qualify Integrated SQLite Workflows and Supported Builds

**What to build:** The combined SQLite-backed backup and restore workflows are verified end to end, and the Go project continues to test and build for its supported platforms.

**Blocked by:** 01: Move Backup Inventory and Priority Planning to SQLite; 02: Move Restore-Context State to SQLite.

**Status:** ready-for-agent

- [ ] An end-to-end backup and multi-volume restore verifies restored file contents and expected backup and restore metadata.
- [ ] The full Go test suite is run; existing failures are distinguished from regressions, and no new failures remain.
- [ ] The existing Linux, macOS, and Windows architecture targets build successfully with the pure-Go SQLite driver and without a CGO toolchain requirement.
- [ ] Tests assert user-visible behavior and public store contracts, not SQLite schema, indexes, query text, or driver internals.
- [ ] Python tests and behavior remain unchanged by the Go migration.
