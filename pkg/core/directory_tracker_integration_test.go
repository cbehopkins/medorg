package core

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDirectoryTrackerIntegrationRealStructure tests with deep nesting, symlinks, and hidden files
func TestDirectoryTrackerIntegrationRealStructure(t *testing.T) {
	dir := t.TempDir()

	// Create deep nesting
	deepPath := filepath.Join(dir, "a", "b", "c", "d", "e", "f")
	if err := os.MkdirAll(deepPath, 0o755); err != nil {
		t.Fatalf("failed to create nested dirs: %v", err)
	}
	if f, err := os.Create(filepath.Join(deepPath, "file.txt")); err != nil {
		t.Fatalf("failed to create file in deep dir: %v", err)
	} else {
		f.Close()
	}

	// Create hidden directories (starting with dot)
	hiddenDir := filepath.Join(dir, ".hidden")
	if err := os.Mkdir(hiddenDir, 0o755); err != nil {
		t.Fatalf("failed to create hidden dir: %v", err)
	}
	if f, err := os.Create(filepath.Join(hiddenDir, "hidden_file.txt")); err != nil {
		t.Fatalf("failed to create file in hidden dir: %v", err)
	} else {
		f.Close()
	}

	// Create multiple directories with files
	for i := 0; i < 5; i++ {
		dirName := filepath.Join(dir, "dir"+string(rune(48+i)))
		if err := os.Mkdir(dirName, 0o755); err != nil {
			t.Fatalf("failed to create directory: %v", err)
		}
		for j := 0; j < 3; j++ {
			fileName := filepath.Join(dirName, "file"+string(rune(48+j))+".txt")
			if f, err := os.Create(fileName); err != nil {
				t.Fatalf("failed to create file: %v", err)
			} else {
				f.Close()
			}
		}
	}

	// Create symlink to a directory (if supported on the platform)
	symDir := filepath.Join(dir, "link")
	targetDir := filepath.Join(dir, "dir0")
	os.Symlink(targetDir, symDir) // Ignore error on Windows where this may not be supported

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
