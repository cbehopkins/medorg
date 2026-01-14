package core

import (
	"os"
	"path/filepath"
	"testing"
)

func TestProgressableDirectoryWalkerVisitsAllFiles(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "sub")

	expected := 0
	expected += writeDirMap(t, root, []string{"root.txt"})
	expected += writeDirMap(t, sub, []string{"child.txt"})

	walker := NewProgressableDirectoryWalker(nil, root)
	fileCount := 0
	walker.AddFileVisitor(func(name Fname, fm FileMetadata, fi os.FileInfo) error {
		fileCount++
		return nil
	})

	if err := walker.Walk(root); err != nil {
		t.Fatalf("walk failed: %v", err)
	}

	if fileCount != expected {
		t.Fatalf("visited %d files, expected %d", fileCount, expected)
	}
}
