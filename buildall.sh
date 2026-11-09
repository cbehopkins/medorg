#!/bin/bash
set -e

# Base directory for binaries - can be overridden via BASE_BIN_DIR env var
BASE_BIN_DIR="${BASE_BIN_DIR:-$HOME/home/git}"

# Install for native OS
go install ./cmd/...

# Define build targets (OS/ARCH pairs and their output subdirectories)
declare -A TARGETS=(
	[linux/amd64]="bin-x86_64"
	[linux/arm64]="bin-aarch64"
	[darwin/amd64]="bin-macos-x86"
	[darwin/arm64]="bin-macos-arm64"
	[windows/amd64]="bin-windows"
)

# List of commands to build
COMMANDS=(mdcalc mdbackup mdrestore mdsource mdjournal mdlabel)

# Build for each target
for target in "${!TARGETS[@]}"; do
	IFS='/' read -r GOOS GOARCH <<< "$target"
	OUTPUT_SUBDIR="${TARGETS[$target]}"
	OUTPUT_DIR="$BASE_BIN_DIR/$OUTPUT_SUBDIR"
	
	echo "Building for $GOOS/$GOARCH → $OUTPUT_DIR"
	mkdir -p "$OUTPUT_DIR"
	
	for cmd in "${COMMANDS[@]}"; do
		echo "  - $cmd"
		GOOS="$GOOS" GOARCH="$GOARCH" go build -o "$OUTPUT_DIR/$cmd" "./cmd/$cmd"
	done
done

echo "Build complete!"
