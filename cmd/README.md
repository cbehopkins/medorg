# Command-Line Tools

This directory contains the executable command-line tools for the medorg project.

## Available Commands

### mdcalc
Performs checksum calculation and various file operations:
- Calculate/update MD5 checksums for files
- Detect and handle duplicate files
- Move detection (find moved files by size/name matching)
- Auto-rename files based on regex rules
- Concentrate files from subdirectories

**Build:** `go build ./cmd/mdcalc`

**Usage examples:**
```bash
# Calculate checksums in current directory
./mdcalc

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

#### Profiling (pprof)
Profiling is opt-in and only included in debug builds using a build tag.

Build with profiling enabled:
```bash
go build -tags debugpprof ./cmd/mdbackup
```

Common runtime flags (debug build only):
- `-pprof-http <addr>`: Start `net/http/pprof` server (e.g. `:6060`, `localhost:6060`)
- `-heap-profile-every <dur>`: Periodic heap/goroutine dumps (e.g. `30s`, `1m`)
- `-mem-threshold-mb <mb>`: Dump profiles when Go Alloc crosses threshold
- `-profile-out <dir>`: Directory for dumped profile files (default `pprof`)
- `-cpu-profile <path>`: Write CPU profile for the run
- `-block-profile-rate`, `-mutex-profile-fraction`, `-mem-profile-rate`: Advanced tuning
- `-dump-profiles-on-interrupt`: On first Ctrl-C, dump heap/allocs/goroutine/mutex/block

Example (Raspberry Pi):
```bash
./mdbackup -pprof-http :6060 \
	-heap-profile-every 30s \
	-mem-threshold-mb 2048 \
	-dump-profiles-on-interrupt \
	-skip-checkcalc /mnt/backup
```

Accessing profiles:
```bash
# Index
curl http://<host>:6060/debug/pprof/

# Heap (binary)
curl -o heap.pprof http://<host>:6060/debug/pprof/heap

# Quick text summary
curl "http://<host>:6060/debug/pprof/heap?debug=1"

# Interactive web UI (runs locally)
go tool pprof -http=:8081 http://<host>:6060/debug/pprof/heap
# Open http://127.0.0.1:8081
```

Safer remote access:
- Prefer SSH tunneling over exposing pprof directly:
```bash
ssh -L 6060:127.0.0.1:6060 user@<host>
# Then browse http://127.0.0.1:6060/debug/pprof/
```
- If you must expose, restrict firewall to your IP/subnet.

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
