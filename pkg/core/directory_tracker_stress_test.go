package core

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDirectoryTrackerConcurrencyStress creates many directories/files to stress test concurrency
func TestDirectoryTrackerConcurrencyStress(t *testing.T) {
	dir := t.TempDir()
	nDirs := 100
	nFiles := 10
	for i := 0; i < nDirs; i++ {
		dirPath := filepath.Join(dir, "d", "dir"+string(rune('0'+i%10))+"_"+string(rune('0'+i/10)))
		if err := os.MkdirAll(dirPath, 0o755); err != nil {
			t.Fatalf("failed to create dir: %v", err)
		}
		for j := 0; j < nFiles; j++ {
			f, err := os.Create(filepath.Join(dirPath, "file"+string(rune('0'+j))))
			if err != nil {
				t.Fatalf("failed to create file: %v", err)
			}
			f.Close()
		}
	}
	makerFunc := func(dir string) (DirectoryTrackerInterface, error) {
		return newMockDtType(), nil
	}
	errChan := NewDirTracker(false, dir, makerFunc).ErrChan()
	for err := range errChan {
		if err != nil {
			t.Error(err)
		}
	}
}
