package main

import (
	"os"
	"path/filepath"
	"testing"
)

// Unit tests for helper functions in main.go

// TestSizeOfNormalFile tests sizeOf with a regular file
func TestSizeOfNormalFile(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	// Create 10KB file
	content := make([]byte, 10*1024)
	if err := os.WriteFile(testFile, content, 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	size := sizeOf(testFile)
	if size != 10 {
		t.Errorf("Expected size 10KB, got %d", size)
	}
}

// TestSizeOfLargeFile tests sizeOf with a large file
func TestSizeOfLargeFile(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "large.bin")

	// Create 100MB file (file size only, sparse if supported)
	if err := os.Truncate(testFile, 100*1024*1024); err != nil {
		// If truncate fails, skip test
		t.Skipf("Cannot create sparse file: %v", err)
	}

	size := sizeOf(testFile)
	expected := 100 * 1024
	if size != expected {
		t.Errorf("Expected size %dKB, got %d", expected, size)
	}
}

// TestSizeOfNonexistentFile tests sizeOf with missing file
func TestSizeOfNonexistentFile(t *testing.T) {
	size := sizeOf("/nonexistent/file/path")
	if size != 0 {
		t.Errorf("Expected 0 for nonexistent file, got %d", size)
	}
}

// TestSizeOfZeroByteFile tests sizeOf with empty file
func TestSizeOfZeroByteFile(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "empty.txt")

	if err := os.WriteFile(testFile, []byte{}, 0o644); err != nil {
		t.Fatalf("Failed to create empty file: %v", err)
	}

	size := sizeOf(testFile)
	if size != 0 {
		t.Errorf("Expected size 0 for empty file, got %d", size)
	}
}

// TestSizeOfDirectory tests sizeOf behavior with directory (should have 0 size)
func TestSizeOfDirectory(t *testing.T) {
	tmpDir := t.TempDir()

	// Try to get size of directory (should return 0 or be harmless)
	size := sizeOf(tmpDir)
	if size < 0 {
		t.Errorf("sizeOf returned negative value for directory: %d", size)
	}
}

// TestSizeOfOverflowBoundary tests sizeOf with file near 2GB boundary
func TestSizeOfOverflowBoundary(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "boundary.bin")

	// Create file slightly over 2GB in size calculation (2GB = 2097152 KB)
	// We create a 2.5GB file to test overflow handling
	sizeBytes := int64(2500 * 1024 * 1024) // 2500 MB
	if err := os.Truncate(testFile, sizeBytes); err != nil {
		t.Skipf("Cannot create large sparse file: %v", err)
	}

	size := sizeOf(testFile)
	maxInt32 := (1 << 31) - 1
	if size != maxInt32 {
		t.Errorf("Expected clamped value %d for overflow, got %d", maxInt32, size)
	}
}

// TestSizeOfSmallFiles tests sizeOf with various small file sizes
func TestSizeOfSmallFiles(t *testing.T) {
	tests := []struct {
		name     string
		sizeKB   int64
		expected int
	}{
		{"1 byte", 1, 0}, // 1 byte rounds to 0 KB
		{"512 bytes", 512, 0},
		{"1 KB", 1024, 1},
		{"2 KB", 2048, 2},
		{"10 KB", 10240, 10},
		{"1 MB", 1024 * 1024, 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			testFile := filepath.Join(tmpDir, "test.bin")

			// Create file first, then truncate it
			if err := os.WriteFile(testFile, []byte("x"), 0o644); err != nil {
				t.Fatalf("Failed to create test file: %v", err)
			}
			if err := os.Truncate(testFile, tt.sizeKB); err != nil {
				t.Fatalf("Failed to truncate file: %v", err)
			}

			size := sizeOf(testFile)
			if size != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, size)
			}
		})
	}
}

// TestSimpleVolumeLabelProvider tests volume label retrieval
func TestSimpleVolumeLabelProvider(t *testing.T) {
	tmpDir := t.TempDir()
	dirs := map[string]string{
		"config": tmpDir,
		"other":  filepath.Join(tmpDir, "other"),
	}

	// Create config
	xc := newXMLCfgAt(t, dirs["config"])
	setupVolumeConfigs(t, xc, dirs["config"])

	provider := SimpleVolumeLabelProvider{xc}
	label, err := provider.GetVolumeLabel(dirs["config"])

	if err != nil {
		t.Errorf("GetVolumeLabel failed: %v", err)
	}
	if label == "" {
		t.Error("Expected non-empty volume label")
	}
}

// TestSimpleVolumeLabelProviderNotFound tests that GetVolumeLabel behaves with unconfigured directory
func TestSimpleVolumeLabelProviderNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	xc := newXMLCfgAt(t, tmpDir)

	provider := SimpleVolumeLabelProvider{xc}
	
	// Try to get label for a different directory that was never set up
	nonexistentDir := "/nonexistent/path/that/does/not/exist"
	_, err := provider.GetVolumeLabel(nonexistentDir)

	// Should error because directory doesn't exist
	if err == nil {
		t.Error("Expected error for nonexistent directory")
	}
}
