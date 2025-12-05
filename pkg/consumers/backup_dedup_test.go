package consumers

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

// TestBackupDuplicateContentFiles tests the scenario where two different files
// (different names) have identical content and therefore the same checksum.
//
// CURRENT BEHAVIOR: Both files are copied to the destination with their original names.
// This test documents the current behavior for future deduplication work.
func TestBackupDuplicateContentFiles(t *testing.T) {
	srcDir, err := os.MkdirTemp("", "dedup_src_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(srcDir)

	dstDir, err := os.MkdirTemp("", "dedup_dst_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dstDir)

	// Create two files with IDENTICAL content but different names
	identicalContent := "This is the exact same content in both files"
	file1 := filepath.Join(srcDir, "document1.txt")
	file2 := filepath.Join(srcDir, "document2.txt")

	if err := os.WriteFile(file1, []byte(identicalContent), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file2, []byte(identicalContent), 0o644); err != nil {
		t.Fatal(err)
	}

	// Create a third file with different content
	file3 := filepath.Join(srcDir, "unique.txt")
	if err := os.WriteFile(file3, []byte("This is unique content"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Calculate checksums
	if err := recalcTestDirectory(srcDir); err != nil {
		t.Fatal(err)
	}

	// Verify that file1 and file2 have the same checksum
	dm, err := core.DirectoryMapFromDir(srcDir)
	if err != nil {
		t.Fatal(err)
	}

	fs1, ok1 := dm.Get("document1.txt")
	fs2, ok2 := dm.Get("document2.txt")
	fs3, ok3 := dm.Get("unique.txt")

	if !ok1 || !ok2 || !ok3 {
		t.Fatal("Failed to get file metadata")
	}

	if fs1.Checksum != fs2.Checksum {
		t.Errorf("Expected file1 and file2 to have same checksum, got %s and %s",
			fs1.Checksum, fs2.Checksum)
	}
	t.Logf("✓ document1.txt and document2.txt have identical checksum: %s", fs1.Checksum)

	if fs1.Checksum == fs3.Checksum {
		t.Error("Unique file should have different checksum")
	}

	// Run backup
	var xc core.MdConfig
	var copyCount uint32
	var copiedFiles []string

	fc := func(src, dst core.Fpath) error {
		atomic.AddUint32(&copyCount, 1)
		copiedFiles = append(copiedFiles, filepath.Base(string(dst)))
		t.Logf("Copying: %s -> %s", src, dst)
		return core.CopyFile(src, dst)
	}

	err = BackupRunner(&xc, 2, fc, srcDir, dstDir, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// CURRENT BEHAVIOR: All 3 files should be copied
	expectedCopies := 3
	if int(copyCount) != expectedCopies {
		t.Errorf("Expected %d files copied, got %d", expectedCopies, copyCount)
	}
	t.Logf("✓ Current behavior: %d files copied (no deduplication)", copyCount)

	// Verify all files exist in destination with their original names
	for _, fn := range []string{"document1.txt", "document2.txt", "unique.txt"} {
		dstFile := filepath.Join(dstDir, fn)
		if _, err := os.Stat(dstFile); os.IsNotExist(err) {
			t.Errorf("File should exist in destination: %s", fn)
		} else {
			t.Logf("✓ File exists in destination: %s", fn)
		}
	}

	// Verify content is preserved
	content1, _ := os.ReadFile(filepath.Join(dstDir, "document1.txt"))
	content2, _ := os.ReadFile(filepath.Join(dstDir, "document2.txt"))
	content3, _ := os.ReadFile(filepath.Join(dstDir, "unique.txt"))

	if string(content1) != identicalContent {
		t.Error("document1.txt content not preserved")
	}
	if string(content2) != identicalContent {
		t.Error("document2.txt content not preserved")
	}
	if string(content1) != string(content2) {
		t.Error("Duplicate files should have identical content")
	}
	if string(content3) == identicalContent {
		t.Error("Unique file should have different content")
	}

	t.Log("✓ All file contents preserved correctly")

	// Calculate space usage
	stat1, _ := os.Stat(filepath.Join(dstDir, "document1.txt"))
	stat2, _ := os.Stat(filepath.Join(dstDir, "document2.txt"))
	stat3, _ := os.Stat(filepath.Join(dstDir, "unique.txt"))

	totalSize := stat1.Size() + stat2.Size() + stat3.Size()
	duplicateSize := stat1.Size() // This is "wasted" space
	t.Logf("Space analysis: Total: %d bytes, Duplicate overhead: %d bytes (%.1f%%)",
		totalSize, duplicateSize, float64(duplicateSize)/float64(totalSize)*100)

	// NOTE: Future deduplication work would reduce total size by not storing document2.txt's content
}

// TestBackupDuplicateContentInSubdirs tests deduplication across subdirectories
func TestBackupDuplicateContentInSubdirs(t *testing.T) {
	srcDir, err := os.MkdirTemp("", "dedup_nested_src_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(srcDir)

	dstDir, err := os.MkdirTemp("", "dedup_nested_dst_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dstDir)

	// Create nested structure with duplicate content
	subdir1 := filepath.Join(srcDir, "photos", "2024")
	subdir2 := filepath.Join(srcDir, "backup", "old")

	if err := os.MkdirAll(subdir1, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(subdir2, 0o755); err != nil {
		t.Fatal(err)
	}

	// Same content in different locations with different names
	duplicateContent := "Exact same photo data repeated in multiple places"
	file1 := filepath.Join(subdir1, "IMG_001.jpg")
	file2 := filepath.Join(subdir2, "copy_of_IMG_001.jpg")

	if err := os.WriteFile(file1, []byte(duplicateContent), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file2, []byte(duplicateContent), 0o644); err != nil {
		t.Fatal(err)
	}

	// Calculate checksums
	if err := recalcTestDirectory(srcDir); err != nil {
		t.Fatal(err)
	}

	// Verify checksums match across subdirectories
	dm1, _ := core.DirectoryMapFromDir(subdir1)
	dm2, _ := core.DirectoryMapFromDir(subdir2)

	fs1, _ := dm1.Get("IMG_001.jpg")
	fs2, _ := dm2.Get("copy_of_IMG_001.jpg")

	if fs1.Checksum != fs2.Checksum {
		t.Errorf("Expected files to have same checksum, got %s and %s",
			fs1.Checksum, fs2.Checksum)
	}
	t.Logf("✓ Files in different subdirectories have matching checksum: %s", fs1.Checksum)

	// Run backup
	var xc core.MdConfig
	var copyCount uint32

	fc := func(src, dst core.Fpath) error {
		atomic.AddUint32(&copyCount, 1)
		t.Logf("Copying: %s", src)
		return core.CopyFile(src, dst)
	}

	err = BackupRunner(&xc, 2, fc, srcDir, dstDir, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Both files should be copied (current behavior)
	if copyCount != 2 {
		t.Errorf("Expected 2 files copied, got %d", copyCount)
	}
	t.Logf("✓ Both files copied despite identical content across subdirectories")

	// Verify directory structure is maintained
	dst1 := filepath.Join(dstDir, "photos", "2024", "IMG_001.jpg")
	dst2 := filepath.Join(dstDir, "backup", "old", "copy_of_IMG_001.jpg")

	for _, path := range []string{dst1, dst2} {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("File should exist: %s", path)
		}
	}
	t.Log("✓ Directory hierarchy preserved correctly")
}

// NOTE: Restore tests for duplicate content are in cmd/mdrestore package
// as they require the full restore infrastructure. The tests above demonstrate
// that the current backup behavior copies all files regardless of content duplication.
//
// Future work: Implement content-based deduplication where:
// - Files with identical checksums could be stored once
// - Hardlinks or reference counting could restore multiple filenames
// - This would reduce backup storage requirements significantly
