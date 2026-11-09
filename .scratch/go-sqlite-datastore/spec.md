Status: ready-for-agent

# Go SQLite Datastore Experiment

## Problem Statement

Large backups run on a Raspberry Pi can exhaust available memory. The Go backup planner currently uses bobbob-backed treap collections for its source inventory, destination inventory, and prioritised copy plan. The Go restore workflow also uses bobbob to retain pending and copied restore tasks across separate command invocations. The storage and persistence machinery make these workflows difficult to reason about and may contribute to memory pressure.

The operator needs to determine whether SQLite can provide the required on-disk working state with lower peak memory while keeping backup and restore behaviour unchanged. This is an experiment, not yet a decision to adopt SQLite permanently.

## Solution

Replace the Go backup planner and Go restore task store with SQLite-backed implementations using a pure-Go driver. Keep the existing command workflows, checksum and `.medorg.xml` contracts, content-based grouping, file-selection policy, and full-destination handling. Keep database work bounded so large inventories and query results are not all materialised in Go memory.

The backup inventory is temporary to one `mdbackup` run. Restore state persists only for the active restore context so `mdrestore newdb` and later `mdrestore copy` invocations can share progress. Python's existing SQLite database remains independent and unchanged.

## User Stories

1. As a backup operator, I want large source and destination inventories stored without retaining every record in Go memory, so that backups can complete on memory-constrained Raspberry Pi systems.
2. As a backup operator, I want the inventory to be built from the existing source files, destination files, and their `.medorg.xml` metadata, so that the SQLite experiment uses the same inputs as the current workflow.
3. As a backup operator, I want missing or stale checksums to continue being calculated by the existing checksum workflow, so that replacing the datastore does not change checksum correctness.
4. As a backup operator, I want content already present on a destination to be recognised by checksum, so that the system does not copy duplicate content unnecessarily.
5. As a backup operator, I want repeated source content to remain grouped by checksum with its backup destinations merged, so that content-based deduplication remains unchanged.
6. As a backup operator, I want a suitable source path retained for each grouped content item, so that the planner can copy the content from a source that needs backup.
7. As a backup operator, I want content with fewer existing backup destinations selected first, so that backup coverage is balanced across volumes.
8. As a backup operator, I want larger eligible files selected first within the established priority ordering, so that limited destination capacity is used predictably.
9. As a backup operator, I want equal-priority candidates to have deterministic ordering, so that repeated runs produce stable, testable plans.
10. As a backup operator, I want candidates already present on the current destination or already meeting the configured backup limit to be excluded, so that work is not repeated unnecessarily.
11. As a backup operator, I want destination-full errors on a large file to allow attempts at smaller candidates, so that usable remaining capacity is not wasted.
12. As a backup operator, I want source `.medorg.xml` metadata updated only for successfully backed-up content, so that source records do not claim a destination that lacks the file.
13. As a backup operator, I want destination `.medorg.xml` metadata updated after successful copies, so that later backup and restore workflows can discover the content and its destination.
14. As a backup operator, I want destination-only content to remain detectable as orphaned, so that existing orphan reporting or deletion behaviour is preserved.
15. As a backup operator, I want the backup database to be created for one backup run and cleaned up when that run closes, so that temporary state does not accumulate between runs.
16. As a restore operator, I want journal entries loaded into pending restore work grouped by content while retaining each distinct target, so that shared content can satisfy multiple restore destinations.
17. As a restore operator, I want pending and copied restore state to survive closing and reopening the database, so that a restore can continue across separate `mdrestore` commands.
18. As a restore operator, I want content copied from one backup volume to leave other targets for that content pending, so that shared content and multiple restore destinations are handled correctly.
19. As a restore operator, I want each target's transition from pending to copied to be atomic in the database after its file copy succeeds, so that the database cannot record a partial state transition.
20. As a restore operator, I want pending work grouped by required backup volume to remain reportable, so that I know which volumes are still needed.
21. As a restore operator, I want repeated copy commands to remain idempotent, so that continuing a restore does not create duplicate completed work.
22. As a restore operator, I want a new restore context to use a new SQLite database while an active context can continue using its existing database, so that completed restore state need not become a permanent archive.
23. As an operator with an existing bobbob restore database, I want the SQLite implementation not to silently interpret that file as SQLite, so that incompatible state is not corrupted.
24. As a maintainer, I want Go SQLite support to use a pure-Go driver, so that the existing cross-platform build targets do not acquire a CGO toolchain requirement.
25. As a maintainer, I want the Go and Python database schemas to remain independent, so that this experiment does not expand into a cross-language schema migration.
26. As a maintainer, I want existing command flags and user-visible workflow stages to remain stable, so that operators can try the experiment without learning a new backup or restore process.
27. As a maintainer, I want peak memory and output correctness measured on a representative large Raspberry Pi workload, so that the experiment can be evaluated against the actual failure rather than assumed benefits.

## Implementation Decisions

- Change only the Go backup planner and Go restore task store. Do not modify the Python implementation or make the two implementations share a schema.
- Use a pure-Go SQLite driver. The specific driver remains an implementation choice, subject to the existing platform build targets and measured memory use.
- Keep the backup inventory transient to one `mdbackup` run. Create and close its SQLite database within that lifecycle and remove the temporary database and associated SQLite files on close.
- Keep restore state persistent across invocations within one active restore context. Opening an existing restore database continues the current behavior; `newdb` does not implicitly clear it. Start a new context with a new database path.
- Do not migrate bobbob restore database files. An in-progress restore using the old format must be completed with the old binary; new SQLite restore contexts use a new database file.
- Keep Python's SQLite database and schema independent from the Go database.
- Preserve the current content identity and grouping semantics: backup inventory is keyed by checksum, repeated content merges backup-destination metadata, and restore inventory groups by checksum and size while retaining distinct targets.
- Express source-versus-destination comparison, orphan detection, eligibility, and priority order in database queries. Iterate query results incrementally and avoid copying whole inventories or candidate sets into Go slices or maps.
- Preserve the established backup priority: fewer existing destinations first, then larger files, then deterministic path ordering. Preserve the configured backup limit and current production behavior at its boundary.
- Preserve the current response to destination-full errors: skip candidates that cannot fit and continue with smaller eligible candidates where the existing workflow permits.
- Keep checksum calculation, filesystem walking, copy operations, volume-label handling, and `.medorg.xml` serialization contracts unchanged. Update source and destination metadata through the existing successful-copy workflow.
- Perform the restore file copy before changing database state. After a successful copy, update pending and copied records in one SQLite transaction. If the transaction fails, report the error and do not mark the target copied.
- Keep CLI arguments and user-facing workflow behaviour stable. SQLite is an implementation detail for these workflows.

## Testing Decisions

- Test externally observable behavior rather than SQL schema, table names, indexes, query text, or driver internals.
- At the `mdbackup.Run` workflow seam, use temporary source and destination trees and verify copied file content, backup selection order where externally visible, source and destination `.medorg.xml` updates, orphan behavior, and continuation to smaller files after a destination-full error.
- At the two-phase `mdrestore newdb` and `copy` workflow seam, close and reopen the database between commands and verify multi-volume continuation, distinct targets for shared content, pending/copied counts and volume reporting, and idempotent repeated copy commands.
- Verify backup database lifecycle cleanup and restore database persistence using public operations rather than inspecting SQLite implementation details.
- Use existing prior art from the backup run tests, destination-capacity tests, restore database persistence tests, two-phase idempotency tests, and the full backup/restore integration workflow.
- Run the existing Go test suite after the replacement. Record pre-existing failures and avoid unrelated repairs; update tests that assert bobbob-specific internals to assert the external contracts above.
- On the representative Raspberry Pi workload, compare peak resident memory and final backup/restore results against the current implementation. The workload and acceptable memory threshold must be recorded when measured; no numeric threshold is assumed by this spec.

## Out of Scope

- Changing the Python implementation, its SQLite database, or cross-language data exchange.
- Migrating existing bobbob restore database files into SQLite.
- Redesigning backup selection, checksum calculation, filesystem walking, journal formats, volume labels, or `.medorg.xml` formats.
- Removing bobbob from unrelated Go components that are outside the backup planner and restore task store.
- Fixing unrelated existing test failures or unrelated memory issues in other parts of the application.
- Declaring SQLite the permanent datastore before the experiment is measured on the target workload.

## Further Notes

- Current evidence points to two separate bobbob-backed stores with different lifecycles: the backup inventory is per-run, while restore progress spans multiple commands in an active restore context.
- Existing production and in-memory backup processor implementations differ at the configured maximum-backup boundary. The SQLite implementation should match the production behavior; tests should exercise the production workflow rather than encode the in-memory fallback's differing boundary.
- This spec captures the experiment's design, not proof that bobbob is the cause of memory exhaustion. Measure the same failing workload to distinguish datastore memory pressure from a leak or accumulation elsewhere in the Go workflow.
