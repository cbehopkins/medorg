package consumers

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

// TestBackupDestinationMetadataRecovery tests the scenario where:
// 1. Backup destination has had all .medorg.xml files deleted/corrupted
// 2. A backup command runs targeting that destination
// 3. The .medorg.xml files are rebuilt
// 4. Files are NOT recopied (idempotent behavior)
func TestBackupDestinationMetadataRecovery(t *testing.T) {
	// Create source directory with some files
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

	// Create test files in source
	testFiles := []string{"file1.txt", "file2.txt", "file3.txt"}
	for _, fn := range testFiles {
		content := "Content of " + fn
		if err := os.WriteFile(filepath.Join(srcDir, fn), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Calculate checksums for source directory
	if err := recalcTestDirectory(srcDir); err != nil {
		t.Fatal(err)
	}

	// STEP 1: Perform initial backup
	t.Log("=== STEP 1: Initial backup ===")
	var xc core.MdConfig
	var initialCopyCount uint32
	fc := func(src, dst core.Fpath) error {
		atomic.AddUint32(&initialCopyCount, 1)
		t.Logf("Initial backup: copying %s -> %s", src, dst)
		return core.CopyFile(src, dst)
	}

	err = BackupRunner(&xc, 2, fc, srcDir, dstDir, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	if initialCopyCount != uint32(len(testFiles)) {
		t.Errorf("Initial backup should copy %d files, copied %d", len(testFiles), initialCopyCount)
	}
	t.Logf("✓ Initial backup completed: %d files copied", initialCopyCount)

	// Verify destination has files and metadata
	for _, fn := range testFiles {
		dstFile := filepath.Join(dstDir, fn)
		if _, err := os.Stat(dstFile); os.IsNotExist(err) {
			t.Errorf("File not copied to destination: %s", fn)
		}
	}

	dstMetadata := filepath.Join(dstDir, core.Md5FileName)
	if _, err := os.Stat(dstMetadata); os.IsNotExist(err) {
		t.Error("Destination metadata file not created")
	}
	t.Log("✓ Destination has files and metadata")

	// STEP 2: Delete all .medorg.xml files from destination (simulate corruption/deletion)
	t.Log("=== STEP 2: Simulating metadata loss ===")
	err = filepath.Walk(dstDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && info.Name() == core.Md5FileName {
			t.Logf("Deleting metadata file: %s", path)
			return os.Remove(path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify metadata is gone
	if _, err := os.Stat(dstMetadata); !os.IsNotExist(err) {
		t.Error("Destination metadata file should be deleted")
	}
	t.Log("✓ All .medorg.xml files deleted from destination")

	// Verify files still exist in destination
	for _, fn := range testFiles {
		dstFile := filepath.Join(dstDir, fn)
		if _, err := os.Stat(dstFile); os.IsNotExist(err) {
			t.Errorf("File should still exist in destination: %s", fn)
		}
	}
	t.Log("✓ Files still present in destination")

	// STEP 3: Run backup again - should rebuild metadata and NOT recopy files
	t.Log("=== STEP 3: Running backup with missing metadata ===")
	var secondCopyCount uint32
	fc2 := func(src, dst core.Fpath) error {
		atomic.AddUint32(&secondCopyCount, 1)
		t.Logf("Second backup: copying %s -> %s", src, dst)
		return core.CopyFile(src, dst)
	}

	err = BackupRunner(&xc, 2, fc2, srcDir, dstDir, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// STEP 4: Verify files were NOT recopied
	if secondCopyCount != 0 {
		t.Errorf("Expected 0 files to be copied (idempotent), but %d were copied", secondCopyCount)
	} else {
		t.Log("✓ No files were recopied (idempotent behavior confirmed)")
	}

	// Verify metadata was rebuilt
	if _, err := os.Stat(dstMetadata); os.IsNotExist(err) {
		t.Error("Destination metadata file should be rebuilt")
	} else {
		t.Log("✓ Destination .medorg.xml rebuilt")
	}

	// Verify destination metadata has correct checksums
	dm, err := core.DirectoryMapFromDir(dstDir)
	if err != nil {
		t.Fatal(err)
	}

	for _, fn := range testFiles {
		fs, ok := dm.Get(fn)
		if !ok {
			t.Errorf("File not in rebuilt metadata: %s", fn)
			continue
		}
		if fs.Checksum == "" {
			t.Errorf("File %s has no checksum in rebuilt metadata", fn)
		} else {
			t.Logf("✓ File %s has checksum in rebuilt metadata: %s", fn, fs.Checksum)
		}
	}
}

// TestBackupDestinationMetadataRecoveryWithSubdirs tests metadata recovery
// with nested directory structures
func TestBackupDestinationMetadataRecoveryWithSubdirs(t *testing.T) {
	// Create source directory with nested structure
	srcDir, err := os.MkdirTemp("", "backup_src_nested_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(srcDir)

	dstDir, err := os.MkdirTemp("", "backup_dst_nested_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dstDir)

	// Create nested structure
	subdir := filepath.Join(srcDir, "subdir")
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create files
	testFiles := map[string]string{
		filepath.Join(srcDir, "root.txt"):   "root level",
		filepath.Join(subdir, "nested.txt"): "nested level",
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

	// Initial backup
	var xc core.MdConfig
	var initialCopyCount uint32
	fc := func(src, dst core.Fpath) error {
		atomic.AddUint32(&initialCopyCount, 1)
		return core.CopyFile(src, dst)
	}

	err = BackupRunner(&xc, 2, fc, srcDir, dstDir, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	expectedFiles := 2
	if int(initialCopyCount) != expectedFiles {
		t.Errorf("Expected %d files copied, got %d", expectedFiles, initialCopyCount)
	}
	t.Logf("✓ Initial backup: %d files", initialCopyCount)

	// Delete all .medorg.xml files from destination AND subdirectories
	deletedCount := 0
	err = filepath.Walk(dstDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && info.Name() == core.Md5FileName {
			t.Logf("Deleting metadata: %s", path)
			deletedCount++
			return os.Remove(path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if deletedCount < 1 {
		t.Errorf("Expected at least 1 metadata file deleted, got %d", deletedCount)
	}
	t.Logf("✓ Deleted %d .medorg.xml files", deletedCount)

	// Run backup again
	var secondCopyCount uint32
	fc2 := func(src, dst core.Fpath) error {
		atomic.AddUint32(&secondCopyCount, 1)
		t.Logf("Unexpected copy: %s -> %s", src, dst)
		return core.CopyFile(src, dst)
	}

	err = BackupRunner(&xc, 2, fc2, srcDir, dstDir, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Verify no files were recopied
	if secondCopyCount != 0 {
		t.Errorf("Expected 0 files recopied, got %d", secondCopyCount)
	} else {
		t.Log("✓ No files recopied despite missing metadata in subdirectories")
	}

	// Verify metadata was rebuilt in all directories
	rebuiltCount := 0
	err = filepath.Walk(dstDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && info.Name() == core.Md5FileName {
			rebuiltCount++
			t.Logf("Metadata rebuilt: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if rebuiltCount < 1 {
		t.Errorf("Expected at least 1 metadata file rebuilt, got %d", rebuiltCount)
	} else {
		t.Logf("✓ Rebuilt %d .medorg.xml files", rebuiltCount)
	}
}

// TestBackupSourceMetadataRecovery tests that if the SOURCE metadata is corrupted,
// the backup process will recalculate it and still work correctly
func TestBackupSourceMetadataRecovery(t *testing.T) {
	srcDir, err := os.MkdirTemp("", "backup_src_recovery_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(srcDir)

	dstDir, err := os.MkdirTemp("", "backup_dst_recovery_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dstDir)

	// Create test files
	testFiles := []string{"file1.dat", "file2.dat", "file3.dat"}
	for _, fn := range testFiles {
		content := "Test content for " + fn
		if err := os.WriteFile(filepath.Join(srcDir, fn), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Calculate checksums for source
	if err := recalcTestDirectory(srcDir); err != nil {
		t.Fatal(err)
	}

	// Verify source metadata exists
	srcMetadata := filepath.Join(srcDir, core.Md5FileName)
	if _, err := os.Stat(srcMetadata); os.IsNotExist(err) {
		t.Error("Source metadata should exist after recalc")
	}

	// Delete source metadata
	t.Log("=== Deleting source metadata ===")
	if err := os.Remove(srcMetadata); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(srcMetadata); !os.IsNotExist(err) {
		t.Error("Source metadata should be deleted")
	}
	t.Log("✓ Source .medorg.xml deleted")

	// Run backup - should recalculate source metadata and perform backup
	var xc core.MdConfig
	var copyCount uint32
	fc := func(src, dst core.Fpath) error {
		atomic.AddUint32(&copyCount, 1)
		return core.CopyFile(src, dst)
	}

	err = BackupRunner(&xc, 2, fc, srcDir, dstDir, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Verify all files were copied
	if int(copyCount) != len(testFiles) {
		t.Errorf("Expected %d files copied, got %d", len(testFiles), copyCount)
	} else {
		t.Logf("✓ All %d files copied successfully", copyCount)
	}

	// Verify source metadata was recreated
	if _, err := os.Stat(srcMetadata); os.IsNotExist(err) {
		t.Error("Source metadata should be recreated")
	} else {
		t.Log("✓ Source .medorg.xml recreated")
	}

	// Verify destination metadata was created
	dstMetadata := filepath.Join(dstDir, core.Md5FileName)
	if _, err := os.Stat(dstMetadata); os.IsNotExist(err) {
		t.Error("Destination metadata should be created")
	} else {
		t.Log("✓ Destination .medorg.xml created")
	}

	// Verify all files exist in destination
	for _, fn := range testFiles {
		dstFile := filepath.Join(dstDir, fn)
		if _, err := os.Stat(dstFile); os.IsNotExist(err) {
			t.Errorf("File not in destination: %s", fn)
		}
	}
	t.Log("✓ All files present in destination")
}
