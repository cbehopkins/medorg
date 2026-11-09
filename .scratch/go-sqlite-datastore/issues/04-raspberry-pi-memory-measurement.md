# 04: Measure SQLite Backup Memory on the Raspberry Pi Workload

**What to build:** The SQLite experiment is evaluated against the current implementation on the representative Raspberry Pi backup workload, with memory use and backup/restore correctness recorded.

**Blocked by:** 01: Move Backup Inventory and Priority Planning to SQLite; 02: Move Restore-Context State to SQLite.

**Status:** ready-for-agent

- [ ] The Raspberry Pi model, available RAM, dataset file count and total size, and relevant backup settings are recorded.
- [ ] Peak resident memory is measured for the current and SQLite implementations using the same workload and configuration.
- [ ] Backup and restore outputs, including file contents and `.medorg.xml` metadata, are checked for parity between implementations.
- [ ] The measured memory change and any correctness differences are recorded without assuming SQLite improves memory use.
- [ ] If the target workload or hardware is unavailable, the missing prerequisite and unmeasured result are recorded rather than claiming success.
