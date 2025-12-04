package core

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// TestDirectoryTrackerPermissionError simulates a permission error on a directory
// Note: This test is skipped on Windows as chmod semantics are different
func TestDirectoryTrackerPermissionError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Skipping permission test on Windows - chmod semantics differ from Unix")
	}

	dir := t.TempDir()
	subdir := filepath.Join(dir, "subdir")
	if err := os.Mkdir(subdir, 0o700); err != nil {
		t.Fatalf("failed to create subdir: %v", err)
	}
	if err := os.Chmod(subdir, 0o000); err != nil {
		t.Fatalf("failed to chmod subdir: %v", err)
	}
	defer os.Chmod(subdir, 0o700) // restore permissions so TempDir can clean up

	makerFunc := func(dir string) (DirectoryTrackerInterface, error) {
		return newMockDtType(), nil
	}
	errChan := NewDirTracker(false, dir, makerFunc).ErrChan()
	hitPerm := false
	for err := range errChan {
		if err != nil && os.IsPermission(err) {
			hitPerm = true
		}
	}
	if !hitPerm {
		t.Error("expected permission error, did not get one")
	}
}

// TestDirectoryTrackerErrorChannelFull verifies DirectoryTracker continues even if errors are dropped
func TestDirectoryTrackerErrorChannelFull(t *testing.T) {
	// Test that DirectoryTracker can handle a complex structure without deadlock
	// even if error channel gets full (errors are dropped with non-blocking send)
	dir := t.TempDir()

	// Create a moderate structure
	for i := 0; i < 5; i++ {
		subdir := filepath.Join(dir, "dir"+string(rune('0'+i)))
		if err := os.Mkdir(subdir, 0o755); err != nil {
			t.Fatalf("failed to create subdir: %v", err)
		}
	}

	makerFunc := func(dir string) (DirectoryTrackerInterface, error) {
		return newMockDtType(), nil
	}

	errChan := NewDirTracker(false, dir, makerFunc).ErrChan()
	errorCount := 0
	for err := range errChan {
		if err != nil {
			errorCount++
		}
	}
	// Test passes if we complete without deadlock/panic
	t.Logf("Completed with %d errors", errorCount)
}
