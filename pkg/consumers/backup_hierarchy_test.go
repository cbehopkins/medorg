package consumers

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

// TestBackupMaintainsDirectoryHierarchy verifies that the backup process
// preserves the source directory structure in the destination
func TestBackupMaintainsDirectoryHierarchy(t *testing.T) {
	// Create source directory with nested structure
	srcDir, err := os.MkdirTemp("", "backup_src_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(srcDir)

	// Create destination directory
	dstDir, err := os.MkdirTemp("", "backup_dst_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dstDir)

	// Create nested directory structure:
	// src/
	//   file1.txt
	//   subdir1/
	//     file2.txt
	//     subdir2/
	//       file3.txt
	//   otherdir/
	//     file4.txt

	// Create subdirectories
	subdir1 := filepath.Join(srcDir, "subdir1")
	subdir2 := filepath.Join(subdir1, "subdir2")
	otherdir := filepath.Join(srcDir, "otherdir")

	for _, dir := range []string{subdir1, subdir2, otherdir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	// Create test files in various directories
	testFiles := map[string]string{
		filepath.Join(srcDir, "file1.txt"):       "root level file",
		filepath.Join(subdir1, "file2.txt"):      "first level subdirectory",
		filepath.Join(subdir2, "file3.txt"):      "second level subdirectory",
		filepath.Join(otherdir, "file4.txt"):     "other directory",
		filepath.Join(subdir1, "another.txt"):    "another file in subdir1",
		filepath.Join(otherdir, "important.dat"): "important data",
	}

	for path, content := range testFiles {
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Calculate checksums for source directory
	if err := recalcTestDirectory(srcDir); err != nil {
		t.Fatal(err)
	}

	// Run backup
	var xc core.MdConfig
	fc := func(src, dst core.Fpath) error {
		return core.CopyFile(src, dst)
	}
	err = BackupRunner(&xc, 2, fc, srcDir, dstDir, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Verify each file exists in the correct location in destination
	for srcPath := range testFiles {
		// Calculate relative path from source
		relPath, err := filepath.Rel(srcDir, srcPath)
		if err != nil {
			t.Fatal(err)
		}

		// Check if file exists at corresponding location in destination
		dstPath := filepath.Join(dstDir, relPath)
		if _, err := os.Stat(dstPath); os.IsNotExist(err) {
			t.Errorf("File not found at expected location: %s", dstPath)
			t.Errorf("  Expected relative path: %s", relPath)
			t.Errorf("  Source file: %s", srcPath)
		} else if err != nil {
			t.Errorf("Error checking file %s: %v", dstPath, err)
		} else {
			t.Logf("✓ File found at correct location: %s", relPath)
		}
	}

	// Verify directory structure exists
	expectedDirs := []string{
		filepath.Join(dstDir, "subdir1"),
		filepath.Join(dstDir, "subdir1", "subdir2"),
		filepath.Join(dstDir, "otherdir"),
	}

	for _, dir := range expectedDirs {
		if stat, err := os.Stat(dir); os.IsNotExist(err) {
			t.Errorf("Expected directory not found: %s", dir)
		} else if err != nil {
			t.Errorf("Error checking directory %s: %v", dir, err)
		} else if !stat.IsDir() {
			t.Errorf("Path exists but is not a directory: %s", dir)
		} else {
			relPath, _ := filepath.Rel(dstDir, dir)
			t.Logf("✓ Directory structure preserved: %s", relPath)
		}
	}

	// Verify content of a couple files to ensure they were copied correctly
	srcFile1 := filepath.Join(srcDir, "subdir1", "subdir2", "file3.txt")
	dstFile1 := filepath.Join(dstDir, "subdir1", "subdir2", "file3.txt")

	srcContent, err := os.ReadFile(srcFile1)
	if err != nil {
		t.Fatal(err)
	}
	dstContent, err := os.ReadFile(dstFile1)
	if err != nil {
		t.Fatal(err)
	}

	if string(srcContent) != string(dstContent) {
		t.Errorf("Content mismatch for nested file:\n  Source: %s\n  Destination: %s",
			string(srcContent), string(dstContent))
	} else {
		t.Log("✓ File content preserved correctly")
	}
}

// TestBackupStreamingMaintainsHierarchy tests that the streaming implementation
// also preserves directory structure
func TestBackupStreamingMaintainsHierarchy(t *testing.T) {
	// Create source with deep nesting
	srcDir, err := os.MkdirTemp("", "stream_src_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(srcDir)

	dstDir, err := os.MkdirTemp("", "stream_dst_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dstDir)

	// Create a deeper structure to test streaming with multiple batches
	// photos/
	//   2024/
	//     01/
	//       img001.jpg
	//     02/
	//       img002.jpg
	//   2025/
	//     12/
	//       img003.jpg

	photos2024_01 := filepath.Join(srcDir, "photos", "2024", "01")
	photos2024_02 := filepath.Join(srcDir, "photos", "2024", "02")
	photos2025_12 := filepath.Join(srcDir, "photos", "2025", "12")

	for _, dir := range []string{photos2024_01, photos2024_02, photos2025_12} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	// Create files
	testFiles := map[string]string{
		filepath.Join(photos2024_01, "img001.jpg"): "photo from January 2024",
		filepath.Join(photos2024_02, "img002.jpg"): "photo from February 2024",
		filepath.Join(photos2025_12, "img003.jpg"): "photo from December 2025",
	}

	for path, content := range testFiles {
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Calculate checksums
	if err := recalcTestDirectory(srcDir); err != nil {
		t.Fatal(err)
	}

	// Run backup (uses streaming implementation)
	var xc core.MdConfig
	fc := func(src, dst core.Fpath) error {
		return core.CopyFile(src, dst)
	}
	err = BackupRunner(&xc, 2, fc, srcDir, dstDir, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Verify all files exist at correct paths
	for srcPath := range testFiles {
		relPath, err := filepath.Rel(srcDir, srcPath)
		if err != nil {
			t.Fatal(err)
		}

		dstPath := filepath.Join(dstDir, relPath)
		if _, err := os.Stat(dstPath); os.IsNotExist(err) {
			t.Errorf("File not found: %s (expected at %s)", relPath, dstPath)
		} else {
			t.Logf("✓ Hierarchical path preserved: %s", relPath)
		}
	}

	// Verify the deepest path
	deepFile := filepath.Join(dstDir, "photos", "2025", "12", "img003.jpg")
	if content, err := os.ReadFile(deepFile); err != nil {
		t.Errorf("Failed to read deeply nested file: %v", err)
	} else if string(content) != "photo from December 2025" {
		t.Errorf("Content mismatch in deeply nested file")
	} else {
		t.Log("✓ Deep nesting (4 levels) preserved correctly")
	}
}
