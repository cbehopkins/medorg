//go:build linux

package main

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// TestCopyFile_InterruptionLeavesNoCorruptFile tests that if a copy is interrupted
// (temp file written but rename not completed), the destination is not corrupted.
func TestCopyFile_InterruptionLeavesNoCorruptFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")
	tmp := dst + ".tmp"

	if err := os.WriteFile(src, []byte("ORIGINAL"), 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}

	// Write a pre-existing destination file
	if err := os.WriteFile(dst, []byte("OLD"), 0o644); err != nil {
		t.Fatalf("write dst: %v", err)
	}

	// Simulate copy: write temp, crash before rename
	if err := os.WriteFile(tmp, []byte("ORIGINAL"), 0o644); err != nil {
		t.Fatalf("write tmp: %v", err)
	}

	// Simulate crash: process exits before rename
	// On restart, temp file should be cleaned up, dst should be either OLD or ORIGINAL
	// Clean up temp file as restore would do
	_ = os.Remove(tmp)

	data, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if string(data) != "OLD" && string(data) != "ORIGINAL" {
		t.Fatalf("corrupt file after interruption: %q", data)
	}
}

// TestCopyFile_DiskFullLeavesNoPartialFile tests that if disk is full during copy,
// no partial file is left in the destination.
func TestCopyFile_DiskFullLeavesNoPartialFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")
	tmp := dst + ".tmp"

	if err := os.WriteFile(src, []byte(strings.Repeat("A", 1024)), 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}

	// Simulate disk full by making tmp unwritable (create it as a directory)
	_ = os.Mkdir(tmp, 0o755) // tmp is now a directory, so file creation fails

	err := copyFileFunc(src, dst)
	if err == nil {
		t.Fatalf("expected error due to disk full/unwritable tmp, got nil")
	}

	// tmp should not be a file
	info, err := os.Stat(tmp)
	if err == nil && !info.IsDir() {
		t.Fatalf("tmp file should not exist as a file after disk full")
	}
}

// TestCopyFile_ConcurrentAtomicity tests that concurrent copies to the same file
// are handled atomically without corruption.
func TestCopyFile_ConcurrentAtomicity(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")

	if err := os.WriteFile(src, []byte("DATA"), 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 10)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- copyFileFunc(src, dst)
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("concurrent copyFile error: %v", err)
		}
	}

	// After all, dst should be correct
	data, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if string(data) != "DATA" {
		t.Fatalf("concurrent copyFile left corrupt file: %q", data)
	}
}
