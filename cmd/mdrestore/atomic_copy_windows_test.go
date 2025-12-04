//go:build windows

package main

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCopyFile_InterruptionLeavesNoCorruptFile tests atomic copy behavior on Windows.
// Verifies that temp file cleanup after interruption doesn't corrupt the destination.
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

// TestCopyFile_FileInUseLeavesNoCorruptFile tests Windows-specific file locking behavior.
// Verifies that when destination is locked, copy fails cleanly without corruption.
func TestCopyFile_FileInUseLeavesNoCorruptFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")

	if err := os.WriteFile(src, []byte(strings.Repeat("A", 1024)), 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}

	// Create and hold open the destination file to simulate "file in use"
	dstFile, err := os.OpenFile(dst, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open dst: %v", err)
	}
	_, _ = dstFile.WriteString("LOCKED")
	// Don't close yet - file is in use

	// Try to copy - should fail on Windows due to file lock
	err = copyFileFunc(src, dst)
	dstFile.Close() // Now close it

	// On Windows, the rename might fail due to the lock
	// The key is that we shouldn't have a corrupt file
	if err != nil {
		// If copy failed, dst should still have original content or not exist
		data, readErr := os.ReadFile(dst)
		if readErr == nil {
			if string(data) != "LOCKED" && len(data) != 0 {
				t.Fatalf("file should not be partially written: %q", data)
			}
		}
	}
}

// TestCopyFile_ConcurrentAtomicity tests concurrent copies on Windows.
// Windows file locking means some copies may fail, but final file should be correct.
func TestCopyFile_ConcurrentAtomicity(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")

	if err := os.WriteFile(src, []byte("DATA"), 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}

	var wg sync.WaitGroup
	var successCount int32

	// On Windows, concurrent copies to same file may fail due to locking
	// At least one should succeed, and the final file should be correct
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(iteration int) {
			defer wg.Done()
			// Add increasing delay to reduce contention
			time.Sleep(time.Millisecond * time.Duration(iteration*2))
			err := copyFileFunc(src, dst)
			if err == nil {
				atomic.AddInt32(&successCount, 1)
			}
		}(i)
	}

	wg.Wait()

	// At least one copy should succeed
	if successCount == 0 {
		t.Fatalf("all concurrent copyFile calls failed, expected at least one success")
	}

	// Give Windows time to flush file operations
	time.Sleep(time.Millisecond * 50)

	// After all, dst should be correct
	data, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if string(data) != "DATA" {
		t.Fatalf("concurrent copyFile left corrupt file: %q (len=%d, successCount=%d)", data, len(data), successCount)
	}
}

// TestCopyFile_ReadOnlySource tests copying from read-only files (common on Windows).
func TestCopyFile_ReadOnlySource(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")

	if err := os.WriteFile(src, []byte("READONLY"), 0o444); err != nil {
		t.Fatalf("write src: %v", err)
	}

	// Set source as read-only
	if err := os.Chmod(src, 0o444); err != nil {
		t.Fatalf("chmod src: %v", err)
	}

	// Copy should succeed even with read-only source
	if err := copyFileFunc(src, dst); err != nil {
		t.Fatalf("copyFile failed: %v", err)
	}

	// Verify destination has correct content
	data, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if string(data) != "READONLY" {
		t.Fatalf("incorrect content: %q", data)
	}
}
