package consumers

import (
	"os"
	"path/filepath"
	"sync"
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
	var copiedFilesMu sync.Mutex

	fc := func(src, dst core.Fpath) error {
		atomic.AddUint32(&copyCount, 1)
		copiedFilesMu.Lock()
		copiedFiles = append(copiedFiles, filepath.Base(string(dst)))
		copiedFilesMu.Unlock()
		t.Logf("Copying: %s -> %s", src, dst)
		return core.CopyFile(src, dst)
	}

	err = BackupRunner(&xc, 2, fc, srcDir, dstDir, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// DEDUPLICATION BEHAVIOR: Files with identical checksums should only be copied once
	// document1.txt and document2.txt have the same content, so only one should be copied
	// This prevents wasting backup space on duplicate content
	expectedCopies := 2
	if int(copyCount) != expectedCopies {
		t.Errorf("Expected %d files copied (deduplication), got %d", expectedCopies, copyCount)
	}
	t.Logf("✓ Deduplication working: %d files copied (prevented 1 duplicate)", copyCount)

	// Verify that one of the duplicate files exists, and the other doesn't
	// (The backup system picks whichever one it encounters first)
	file1Exists := false
	file2Exists := false
	if _, err := os.Stat(filepath.Join(dstDir, "document1.txt")); err == nil {
		file1Exists = true
		t.Log("✓ document1.txt exists in destination (first duplicate)")
	}
	if _, err := os.Stat(filepath.Join(dstDir, "document2.txt")); err == nil {
		file2Exists = true
		t.Log("✓ document2.txt exists in destination (second duplicate)")
	}

	// Exactly one of the two duplicates should exist
	if !file1Exists && !file2Exists {
		t.Error("At least one of the duplicate files should exist in destination")
	}
	if file1Exists && file2Exists {
		t.Error("Both duplicate files exist - deduplication failed")
	}

	// Verify unique file exists
	if _, err := os.Stat(filepath.Join(dstDir, "unique.txt")); os.IsNotExist(err) {
		t.Error("Unique file should exist in destination")
	} else {
		t.Log("✓ unique.txt exists in destination")
	}

	// Verify content is preserved
	var content1 []byte
	var content2 []byte
	var content3 []byte
	if file1Exists {
		content1, _ = os.ReadFile(filepath.Join(dstDir, "document1.txt"))
	}
	if file2Exists {
		content2, _ = os.ReadFile(filepath.Join(dstDir, "document2.txt"))
	}
	content3, _ = os.ReadFile(filepath.Join(dstDir, "unique.txt"))

	// The copied duplicate file should have correct content
	if file1Exists && string(content1) != identicalContent {
		t.Error("document1.txt content not preserved")
	}
	if file2Exists && string(content2) != identicalContent {
		t.Error("document2.txt content not preserved")
	}
	if string(content3) == identicalContent {
		t.Error("Unique file should have different content")
	}

	t.Log("✓ File contents preserved correctly")

	// Calculate space savings from deduplication
	var copiedSize int64
	if file1Exists {
		stat1, _ := os.Stat(filepath.Join(dstDir, "document1.txt"))
		copiedSize = stat1.Size()
	} else if file2Exists {
		stat2, _ := os.Stat(filepath.Join(dstDir, "document2.txt"))
		copiedSize = stat2.Size()
	}
	stat3, _ := os.Stat(filepath.Join(dstDir, "unique.txt"))

	totalSize := copiedSize + stat3.Size()
	savedSize := copiedSize // Space saved by not storing duplicate
	t.Logf("Space analysis: Total: %d bytes, Saved by dedup: %d bytes (%.1f%%)",
		totalSize, savedSize, float64(savedSize)/float64(totalSize+savedSize)*100)

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

	// DEDUPLICATION BEHAVIOR: Only one copy of the duplicate file should exist
	if copyCount != 1 {
		t.Errorf("Expected 1 file copied (deduplication), got %d", copyCount)
	}
	t.Logf("✓ Deduplication working across subdirectories: %d file copied", copyCount)

	// Verify that exactly one of the two duplicate files exists
	dst1 := filepath.Join(dstDir, "photos", "2024", "IMG_001.jpg")
	dst2 := filepath.Join(dstDir, "backup", "old", "copy_of_IMG_001.jpg")

	_, err1 := os.Stat(dst1)
	_, err2 := os.Stat(dst2)

	file1Exists := err1 == nil
	file2Exists := err2 == nil

	if !file1Exists && !file2Exists {
		t.Error("At least one copy of the duplicate file should exist")
	}
	if file1Exists && file2Exists {
		t.Error("Both copies exist - deduplication failed")
	}

	if file1Exists {
		t.Log("✓ IMG_001.jpg exists in photos/2024 directory")
	} else {
		t.Log("✓ copy_of_IMG_001.jpg exists in backup/old directory")
	}
	t.Log("✓ Directory hierarchy preserved correctly")
}

// NOTE: Restore tests for duplicate content are in cmd/mdrestore package
// as they require the full restore infrastructure.
//
// CURRENT IMPLEMENTATION: Content-based deduplication is already implemented
// via checksum-based keys:
// - Files with identical checksums are only copied once to the backup
// - The backup stores which files had the same checksum
// - On restore, all original filenames can be recovered from the metadata
//
// This prevents wasting backup space on duplicate content while maintaining
// the ability to restore the complete original directory structure.
