package core

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDeleteMissingFiles verifies that DeleteMissingFiles correctly removes
// file entries that exist in the DirectoryMap but not on disk
func TestDeleteMissingFiles(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir := t.TempDir()

	// Create some test files on disk
	file1 := filepath.Join(tmpDir, "file1.txt")
	file2 := filepath.Join(tmpDir, "file2.txt")

	// Create the actual files on disk
	for _, f := range []string{file1, file2} {
		if err := os.WriteFile(f, []byte("test content"), 0o644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}
	}

	// Create a DirectoryMap and add file entries
	dm := NewDirectoryMap()

	// Add all three files to the map (file3 will not exist on disk)
	dm.Add(FileStruct{Name: "file1.txt", directory: tmpDir})
	dm.Add(FileStruct{Name: "file2.txt", directory: tmpDir})
	dm.Add(FileStruct{Name: "file3.txt", directory: tmpDir}) // This one doesn't exist on disk

	// Verify all three files are in the map before deletion
	if dm.Len() != 3 {
		t.Errorf("expected 3 files in map, got %d", dm.Len())
	}

	// Call DeleteMissingFiles
	err := dm.DeleteMissingFiles()
	if err != nil {
		t.Fatalf("DeleteMissingFiles failed: %v", err)
	}

	// Verify that only the files that exist on disk remain in the map
	if dm.Len() != 2 {
		t.Errorf("expected 2 files in map after deletion, got %d", dm.Len())
	}

	// Verify that file3.txt was removed
	if _, exists := dm.Get("file3.txt"); exists {
		t.Error("file3.txt should have been removed from the map")
	}

	// Verify that file1.txt and file2.txt still exist in the map
	if _, exists := dm.Get("file1.txt"); !exists {
		t.Error("file1.txt should still exist in the map")
	}
	if _, exists := dm.Get("file2.txt"); !exists {
		t.Error("file2.txt should still exist in the map")
	}
}

// TestDeleteMissingFilesEmpty verifies that DeleteMissingFiles handles
// an empty DirectoryMap correctly
func TestDeleteMissingFilesEmpty(t *testing.T) {
	dm := NewDirectoryMap()

	// Call DeleteMissingFiles on empty map
	err := dm.DeleteMissingFiles()
	if err != nil {
		t.Fatalf("DeleteMissingFiles on empty map failed: %v", err)
	}

	// Verify map is still empty
	if dm.Len() != 0 {
		t.Errorf("expected 0 files in map, got %d", dm.Len())
	}
}

// TestDeleteMissingFilesAllExist verifies that DeleteMissingFiles preserves
// all files when they all exist on disk
func TestDeleteMissingFilesAllExist(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test files on disk
	file1 := filepath.Join(tmpDir, "file1.txt")
	file2 := filepath.Join(tmpDir, "file2.txt")

	for _, f := range []string{file1, file2} {
		if err := os.WriteFile(f, []byte("test content"), 0o644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}
	}

	// Create a DirectoryMap with files that all exist
	dm := NewDirectoryMap()
	dm.Add(FileStruct{Name: "file1.txt", directory: tmpDir})
	dm.Add(FileStruct{Name: "file2.txt", directory: tmpDir})

	// Verify we have 2 files
	if dm.Len() != 2 {
		t.Errorf("expected 2 files in map, got %d", dm.Len())
	}

	// Call DeleteMissingFiles
	err := dm.DeleteMissingFiles()
	if err != nil {
		t.Fatalf("DeleteMissingFiles failed: %v", err)
	}

	// Verify all files are still in the map
	if dm.Len() != 2 {
		t.Errorf("expected 2 files in map after deletion, got %d", dm.Len())
	}

	if _, exists := dm.Get("file1.txt"); !exists {
		t.Error("file1.txt should still exist in the map")
	}
	if _, exists := dm.Get("file2.txt"); !exists {
		t.Error("file2.txt should still exist in the map")
	}
}

// TestDeleteMissingFilesAllMissing verifies that DeleteMissingFiles removes
// all files when none exist on disk
func TestDeleteMissingFilesAllMissing(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a DirectoryMap with files that don't exist on disk
	dm := NewDirectoryMap()
	dm.Add(FileStruct{Name: "missing1.txt", directory: tmpDir})
	dm.Add(FileStruct{Name: "missing2.txt", directory: tmpDir})
	dm.Add(FileStruct{Name: "missing3.txt", directory: tmpDir})

	// Verify we have 3 files
	if dm.Len() != 3 {
		t.Errorf("expected 3 files in map, got %d", dm.Len())
	}

	// Call DeleteMissingFiles
	err := dm.DeleteMissingFiles()
	if err != nil {
		t.Fatalf("DeleteMissingFiles failed: %v", err)
	}

	// Verify all files were removed
	if dm.Len() != 0 {
		t.Errorf("expected 0 files in map after deletion, got %d", dm.Len())
	}
}
