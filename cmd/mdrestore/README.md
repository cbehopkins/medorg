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

Then restore files from a backup volume:

```bash
# Restore from backup using journal file
mdrestore --journal backup.journal /mnt/backup1
```

## How it works

1. **Read volume label** - Reads the volume label from the source directory (backup location)
2. **Parse journal** - Reads the journal file to understand what files should exist
3. **Map aliases** - Maps each alias in the journal to a restore destination from MdConfig
4. **Calculate checksums** - Runs check_calc on each restore destination
5. **Compare and copy** - For each file in the journal:
   - Checks if file exists in destination with correct MD5
   - If not, and the file's volume matches current source volume, copies from source
   - If not on current volume, notes which volume has the file
6. **Report missing volumes** - After processing, prints list of volumes needed to complete restore

## Multi-volume restore

If your backup spans multiple volumes, mdrestore will:

1. Restore all files it can find on the current volume
2. Report which other volumes are needed
3. You can then mount each volume and run mdrestore again

Example:

```bash
# Restore from first volume
mdrestore --journal backup.journal /mnt/backup1

# Output shows you need volumes: BACKUP_VOL2, BACKUP_VOL3
# Mount volume 2 and run again
mdrestore --journal backup.journal /mnt/backup2

# Mount volume 3 and run again
mdrestore --journal backup.journal /mnt/backup3
```

## Exit codes

- `0` - Success
- `1` - No config file
- `2` - Invalid arguments
- `3` - Journal file not found
- `4` - Source directory not found
- `5` - Restore error
