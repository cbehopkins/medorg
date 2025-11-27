# Command-Line Tools

This directory contains the executable command-line tools for the medorg project.

## Available Commands

### check_calc
Performs checksum calculation and various file operations:
- Calculate/update MD5 checksums for files
- Detect and handle duplicate files
- Move detection (find moved files by size/name matching)
- Auto-rename files based on regex rules
- Concentrate files from subdirectories

**Build:** `go build ./cmd/check_calc`

**Usage examples:**
```bash
# Calculate checksums in current directory
./check_calc

# Enable rename functionality
./check_calc -rnm

# Enable move detection
./check_calc -mvd

# Delete duplicate files
./check_calc -del
```

### mdbackup
Main backup utility for managing backups across multiple volumes:
- Scan and compare source and destination directories
- Copy files based on priority (backup coverage, size)
- Update volume labels and track backup locations
- Generate backup statistics

**Build:** `go build ./cmd/mdbackup`

**Usage examples:**
```bash
# Run backup from source to destination
./mdbackup /path/to/source /path/to/backup

# Tag a directory (create/show volume label)
./mdbackup -tag /path/to/backup

# Show statistics only (no backup)
./mdbackup -stats /path/to/source /path/to/backup

# Dry run (show what would be done)
./mdbackup -dummy /path/to/source /path/to/backup
```

### mdjournal
Journal-based directory scanner:
- Scan directories and create journal entries
- Track directory structure changes

**Build:** `go build ./cmd/mdjournal`

**Usage examples:**
```bash
# Scan current directory
./mdjournal

# Scan specific directories  
./mdjournal /path/to/dir1 /path/to/dir2

# Scan mode
./mdjournal -scan /path/to/directory
```

## Building All Commands

```bash
# Build all commands
go build ./cmd/...

# Install all commands to $GOPATH/bin
go install ./cmd/...
```

## Dependencies

All commands depend on the main `github.com/cbehopkins/medorg` package which must be in the parent directory.
