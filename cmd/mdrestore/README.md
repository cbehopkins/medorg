# mdrestore

Restore files from backup using journal files.

## Usage

Before restoring, configure restore destinations using `mdsource`:

```bash
# Configure where files with 'media' alias should be restored to
mdsource restore -alias media -path /restore/to/here

# Or use default (original source path)
mdsource restore -alias media
```

Load the journal into a restore database, then scan each available backup
volume. Keep using the same database while restoring across volumes:

```bash
# Load restore targets from the journal
mdrestore newdb --journal backup.journal --db restore.db

# Restore from available volumes
mdrestore copy --db restore.db /mnt/backup1
mdrestore copy --db restore.db /mnt/backup2
```

## How it works

1. **Load journal targets** - `newdb` resolves each journal alias to a restore destination and stores pending targets in SQLite.
2. **Reconcile destinations** - `copy` trusts a named `.medorg.xml` checksum when its recorded size and modification time are unchanged, matching the `mdcalc` cache rule. Otherwise it calculates the destination checksum. Exact matches move to copied state without a source-volume scan when no pending targets remain.
3. **Scan a volume** - `copy` matches files on the supplied volume to remaining pending targets by MD5 and size.
4. **Stream and verify** - Each matching file is streamed to its destination while calculating MD5; the copied size and checksum must match the journal.
5. **Track progress** - Successful copies move to copied state. Failed or mismatched copies are removed and remain pending for retry.

## Multi-volume restore

Run `copy` once for each available backup volume, in any order, using the same
restore database. Pending targets remain available for later volumes and retries.

Use `--dry-run` with `copy` to display planned operations without writing files.
Both subcommands accept `--config`; `newdb` accepts `--journal`, and both accept
`--db` (default: `restore.db`).

## Exit codes

Exit codes use the shared definitions in `pkg/cli`: invalid arguments (`1`),
configuration error (`2`), journal not found (`9`), source not found (`10`),
and restore error (`11`).
