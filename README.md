---
title: "Media Organiser"
---
# medorg - Media Organization and Backup System

A comprehensive suite of utilities for managing large media collections with checksum-based integrity verification, multi-volume backup support, and intelligent restore capabilities.

## Overview

medorg provides a complete workflow for backing up large file collections across multiple volumes and restoring them with confidence. The system uses MD5 checksums to ensure file integrity and track files across backup volumes.

**Core Features:**
- **Checksum-based integrity** - Every file tracked with MD5 checksum, size, and timestamp
- **Multi-volume backup** - Backup collections larger than any single backup medium
- **Smart deduplication** - Automatically detect and handle duplicate files
- **Journal-based restore** - Track which volumes contain which files
- **Idempotent operations** - Safe to re-run without duplicating work

**How it works:**
- Each directory maintains a `.md5_list.xml` file recording checksums for all files
- A configuration file `.medorg.xml` (in current directory or `~/.medorg.xml`) manages source directories and restore destinations
- Backup volumes are labeled and tracked in journal files
- Files can be restored from any combination of backup volumes

# Project Structure

The project follows standard Go layout conventions:

- `cmd/` - Command-line executable tools
  - `cmd/mdcalc/` - Checksum calculation and file operations
  - `cmd/mdsource/` - Source directory and restore destination management
  - `cmd/mdbackup/` - Multi-volume backup management
  - `cmd/mdjournal/` - Directory journaling and tracking
  - `cmd/mdrestore/` - Journal-based restore operations
- `pkg/core/` - Core library code (checksums, file operations, XML handling)
- `pkg/consumers/` - Higher-level operations (backup, restore workflows)
- `py/` - Python implementation of some tools (legacy)

## Quick Start

```bash
# Build all commands
go build ./cmd/...

# Or install to $GOPATH/bin
go install ./cmd/...

# Run tests
go test ./...
```

## Typical Backup/Restore Workflow

### Initial Setup

```bash
# 1. Configure source directories with aliases
mdsource add -path /media/photos -alias photos
mdsource add -path /media/videos -alias videos
mdsource add -path /media/music -alias music

# 2. Configure restore destinations (where files should go when restored)
mdsource restore -alias photos -path /restore/photos
mdsource restore -alias videos -path /restore/videos
mdsource restore -alias music -path /restore/music

# 3. Verify configuration
mdsource list
```

### Creating Backups

```bash
# Calculate checksums for source directories
mdcalc /media/photos /media/videos /media/music

# Create journal to track backup state
mdjournal --scan /media/photos --scan /media/videos --scan /media/music \
          --output backup-journal.xml

# Backup to first volume
mdbackup /media/photos /backup/volume1
mdbackup /media/videos /backup/volume1
# Continue until volume1 is full...

# Backup remaining files to second volume
mdbackup /media/photos /backup/volume2
mdbackup /media/videos /backup/volume2
mdbackup /media/music /backup/volume2

# The backup process:
# - Labels each volume (e.g., VOL_1, VOL_2)
# - Updates source .md5_list.xml to track which volume has each file
# - Skips files already on the destination volume
# - Prioritizes files not yet backed up
```

### Restoring from Backups

```bash
# Restore from first volume
mdrestore --journal backup-journal.xml /backup/volume1

# Output shows:
#   - Files restored: 150
#   - Files skipped (already correct): 0
#   - Missing volumes needed: VOL_2

# Restore from second volume to complete restoration
mdrestore --journal backup-journal.xml /backup/volume2

# Output shows:
#   - Files restored: 89
#   - Files skipped (already correct): 150  (from volume1)
#   - Restore complete!

# The restore process:
# - Checks existing files by checksum (idempotent - safe to re-run)
# - Only copies files that don't exist or have wrong checksums
# - Reports which volumes are needed for complete restore
# - Can restore from volumes in any order
```

### Incremental Backup Updates

```bash
# After adding/modifying files in source:

# 1. Recalculate checksums for changed files
mdcalc /media/photos

# 2. Update journal
mdjournal --scan /media/photos --output updated-journal.xml

# 3. Backup new/changed files
mdbackup /media/photos /backup/volume3

# 4. Files already backed up to volume1 or volume2 are automatically skipped
```

### Advanced Usage

```bash
# Recalculate all checksums (useful after moving files)
mdcalc --recalc /media/photos

# Validate existing checksums
mdcalc --validate /media/photos

# Move detection (faster than recalculation when files have moved)
mdcalc --mvd /media/photos

# Auto-rename files based on configured rules
mdcalc --rename /media/photos

# Use custom configuration file
mdbackup --config /path/to/custom.xml /source /destination
```

## Configuration

All commands support the `--config` flag to specify a custom configuration file location:

```bash
mdcalc --config /path/to/config.xml /path/to/scan
mdjournal --config /path/to/config.xml --scan /source/path
mdbackup --config /path/to/config.xml /source /destination
mdsource --config /path/to/config.xml list
mdrestore --config /path/to/config.xml --journal journal.xml /volume
```

If not specified, commands look for `.medorg.xml` in the current directory or home directory (`~/.medorg.xml`).

The configuration file stores:
- Source directory paths and aliases
- Restore destination mappings
- Volume labels
- Auto-rename regex rules
- Backup priority settings

# Command Reference

## mdcalc - Checksum Calculation

Calculate and maintain MD5 checksums for files in directories.

**Basic usage:**
```bash
# Calculate checksums for current directory
mdcalc

# Calculate for specific directories
mdcalc /path/to/media /another/path

# Recalculate all checksums (even if timestamps haven't changed)
mdcalc --recalc /path/to/media

# Validate existing checksums against actual file contents
mdcalc --validate /path/to/media

# Move detection (find moved files by size/name matching)
mdcalc --mvd /path/to/media

# Auto-rename files based on configured regex rules
mdcalc --rename /path/to/media

# Control parallelism (default: 2)
mdcalc --calc 4 /path/to/media
```

**Features:**
- Recalculates checksums when file size or timestamp changes
- Detects duplicate files (same size and checksum)
- Move detection avoids recalculating checksums for moved files
- Auto-rename with regex-based rules and collision handling
- Concentrate: move files from subdirectories to parent directory

---

### Performance & Memory

The backup system is optimized for large collections:

- **Streaming Architecture**: Files are processed in batches (1000 at a time) rather than loading all file paths into memory at once
- **Memory Efficient**: Suitable for backing up millions of files without excessive RAM usage
- **Parallel Processing**: Uses goroutines with controlled concurrency (2 concurrent copies) for efficient throughput
- **Checksum-Verified**: All copies are verified using MD5 checksums stored in `.medorg.xml` files

For very large collections (e.g., 1TB+ with millions of files), the streaming approach ensures bounded memory usage throughout the backup process.

---

## mdsource - Source Management

Manage source directories and restore destinations.

**Commands:**
```bash
# Add a source directory with alias
mdsource add -path /media/photos -alias photos

# Remove a source directory
mdsource remove -alias photos

# List all configured sources
mdsource list

# Configure restore destination for an alias
mdsource restore -alias photos -path /restore/photos

# Use default restore destination (same as source path)
mdsource restore -alias photos
```

Source aliases are used in:
- Journal files to identify which source directory a file came from
- Backup operations to track file origins
- Restore operations to determine destination paths

---

## mdjournal - Directory Journaling

Create journal files that record the current state of directories for backup/restore tracking.

**Usage:**
```bash
# Create journal for single directory
mdjournal --scan /media/photos --output photos.xml

# Create journal for multiple directories
mdjournal --scan /media/photos --scan /media/videos \
          --output backup-2024-12.xml

# Use configured source directories
mdjournal --output full-backup.xml
```

Journal files record:
- All files in scanned directories
- File checksums, sizes, and metadata
- Which backup volumes contain each file
- Source directory aliases

---

## mdbackup - Multi-Volume Backup

Backup files to volumes, tracking which files are on which volumes.

**Usage:**
```bash
# Backup source to destination
mdbackup /media/photos /backup/volume1

# Multiple sources to same destination
mdbackup /media/photos /backup/volume1
mdbackup /media/videos /backup/volume1

# Force recalculation of all checksums
mdbackup --recalc /media/photos /backup/volume1
```

**How it works:**
1. Labels the destination volume (or uses existing label)
2. Calculates checksums for source and destination
3. Detects duplicates between source and destination
4. Updates source `.md5_list.xml` to mark files already on this volume
5. Copies files not yet on this volume, prioritizing:
   - Files not backed up anywhere
   - Larger files (by configured size categories)
6. Skips files already on this volume

**Multi-volume strategy:**
Fill volume1, then volume2, etc. The system automatically tracks which volumes have which files.

---

## mdrestore - Restore from Backup

Restore files from backup volumes using journal files.

**Usage:**
```bash
# Restore from a volume
mdrestore --journal backup.xml /backup/volume1

# Restore from another volume to complete restoration
mdrestore --journal backup.xml /backup/volume2

# Custom configuration
mdrestore --config /path/to/config.xml --journal backup.xml /backup/volume1
```

**How it works:**
1. Reads volume label from backup volume
2. Parses journal to determine which files should be restored
3. Maps aliases to restore destinations (configured via `mdsource restore`)
4. Calculates checksums for existing files at destinations
5. Copies files that don't exist or have incorrect checksums
6. Reports which additional volumes are needed

**Key features:**
- **Idempotent**: Safe to re-run - skips files with correct checksums
- **Multi-volume**: Run once per volume, in any order
- **Smart restore**: Only copies what's needed
- **Progress tracking**: Shows what's restored and what's still needed

# Technical Details

## File Tracking

Each directory maintains a `.md5_list.xml` file containing:
- File name, size, timestamp
- MD5 checksum
- Backup volume references (which volumes have this file)
- Other metadata

## Duplicate Detection

Two files are considered duplicates if they have:
- Identical MD5 checksums
- Identical file sizes

File names and locations don't matter for duplicate detection.

## Auto-Rename Rules

Regex-based renaming with group matching:
- `^ (.*)$` - Remove leading spaces
- `^(.*)[ _-]$` - Remove trailing whitespace, underscores, hyphens
- `^(.*)[|](.*)$` - Remove pipe characters

Rules are applied after extension extraction. Collision handling uses auto-numbering.

## Backup Priority

Files are prioritized for backup by:
1. **Number of backup volumes** - Files not backed up anywhere get highest priority
2. **Size category** - Larger files copied first (configurable size thresholds)

Files already on the destination volume are automatically skipped.

---

# Development

## Running Tests

```bash
# All tests
go test ./...

# Specific package
go test ./pkg/core
go test ./cmd/mdrestore

# With coverage
go test -cover ./...
go test -coverprofile=coverage.out ./cmd/mdrestore
go tool cover -html=coverage.out
```

## Project Status

✅ **Implemented:**
- Multi-volume backup system
- Checksum-based integrity verification
- Journal-based restore
- Source/destination management
- Duplicate detection
- Move detection
- Auto-rename functionality

🚧 **In Progress:**
- GUI for file tagging
- Improved memory efficiency for large backup operations

## Contributing

See individual package documentation in:
- `pkg/core/` - Core functionality
- `pkg/consumers/` - High-level operations
- `cmd/*/` - Command implementations
