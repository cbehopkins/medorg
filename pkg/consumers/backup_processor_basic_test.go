package consumers

import (
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

// TestBackupProcessorBasicCreate tests that we can create a BackupProcessor
func TestBackupProcessorBasicCreate(t *testing.T) {
	bp, err := NewBackupProcessor()
	if err != nil {
		t.Fatalf("Failed to create BackupProcessor: %v", err)
	}
	defer bp.Close()

	t.Log("BackupProcessor created successfully")
}

// TestBackupProcessorAddSingleFile tests adding a single file
func TestBackupProcessorAddSingleFile(t *testing.T) {
	bp, err := NewBackupProcessor()
	if err != nil {
		t.Fatalf("Failed to create BackupProcessor: %v", err)
	}
	defer func() {
		closeErr := bp.Close()
		if closeErr != nil {
			t.Logf("Close error: %v", closeErr)
		}
	}()

	// Add one simple file
	md5Hex := "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"
	file := core.NewFpath("/test", "file.txt")

	err = bp.addSrcFile(md5Hex, 1024, []string{}, file)
	if err != nil {
		t.Fatalf("Failed to add file: %v", err)
	}

	t.Log("Single file added successfully")
}

// TestBackupProcessorAddMultipleFiles tests adding several files sequentially
func TestBackupProcessorAddMultipleFiles(t *testing.T) {
	bp, err := NewBackupProcessor()
	if err != nil {
		t.Fatalf("Failed to create BackupProcessor: %v", err)
	}
	defer bp.Close()

	// Add 10 files with different MD5s
	for i := 0; i < 10; i++ {
		// Create a unique MD5-like hex string (32 chars)
		md5Hex := ""
		for j := 0; j < 32; j++ {
			md5Hex += string("0123456789abcdef"[(i*j)%16])
		}

		file := core.NewFpath("/test/dir", "file"+string(rune(i))+".txt")
		err := bp.addSrcFile(md5Hex, int64(1000+i*100), []string{}, file)
		if err != nil {
			t.Fatalf("Failed to add file %d: %v", i, err)
		}
	}

	t.Log("Multiple files added successfully")
}

// TestBackupProcessorCloseWithoutOperations tests that Close works even with no operations
func TestBackupProcessorCloseWithoutOperations(t *testing.T) {
	bp, err := NewBackupProcessor()
	if err != nil {
		t.Fatalf("Failed to create BackupProcessor: %v", err)
	}

	// Just close immediately without doing anything
	closeErr := bp.Close()
	if closeErr != nil {
		t.Logf("Close returned error (may be expected): %v", closeErr)
	}

	t.Log("Close without operations completed")
}
