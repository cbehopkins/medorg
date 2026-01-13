package core

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// helper to create files and persist a directory map for a directory
func writeDirMap(t *testing.T, dir string, files []string) int {
    t.Helper()
    dm := NewDirectoryMap()
    count := 0
    for _, name := range files {
        // ensure directory exists
        if err := os.MkdirAll(dir, 0o755); err != nil {
            t.Fatalf("mkdir %s: %v", dir, err)
        }
        fp := filepath.Join(dir, name)
        if err := os.WriteFile(fp, []byte("data"), 0o644); err != nil {
            t.Fatalf("write file %s: %v", fp, err)
        }
        fs, err := NewFileStruct(dir, name)
        if err != nil {
            t.Fatalf("NewFileStruct for %s: %v", fp, err)
        }
        dm.Add(fs)
        count++
    }
    if err := dm.Persist(Dirname(dir)); err != nil {
        t.Fatalf("persist dm for %s: %v", dir, err)
    }
    return count
}

func TestDirectoryWalkerVisitsAllFiles(t *testing.T) {
    root := t.TempDir()
    sub := filepath.Join(root, "sub")

    expected := 0
    expected += writeDirMap(t, root, []string{"root.txt"})
    expected += writeDirMap(t, sub, []string{"child.txt"})

    walker := NewDirectoryWalker(nil)
    visited := make(map[string]struct{})
    walker.FileVisitor = func(name string, fm FileMetadata) error {
        visited[filepath.Join(string(fm.Directory()), name)] = struct{}{}
        return nil
    }

    if err := walker.Walk(root); err != nil {
        t.Fatalf("walk failed: %v", err)
    }

    if len(visited) != expected {
        t.Fatalf("visited %d files, expected %d", len(visited), expected)
    }
    want := []string{filepath.Join(root, "root.txt"), filepath.Join(sub, "child.txt")}
    for _, w := range want {
        if _, ok := visited[w]; !ok {
            t.Fatalf("missing visit for %s", w)
        }
    }
}

func TestDirectoryWalkerHandlesEmptyDirectory(t *testing.T) {
    dir := t.TempDir()

    // persist an empty directory map so the walker has metadata to read
    dm := NewDirectoryMap()
    if err := dm.Persist(Dirname(dir)); err != nil {
        t.Fatalf("persist empty dm: %v", err)
    }

    walker := NewDirectoryWalker(nil)
    calls := 0
    walker.FileVisitor = func(name string, fm FileMetadata) error {
        calls++
        return nil
    }

    if err := walker.Walk(dir); err != nil {
        t.Fatalf("walk failed: %v", err)
    }
    if calls != 0 {
        t.Fatalf("expected no visits for empty directory, got %d", calls)
    }
}

func buildLargeTree(t *testing.T, dir string, depth, fanout int) int {
    t.Helper()
    if depth == 0 {
        return 0
    }

    files := []string{"a.txt", "b.txt"}
    total := writeDirMap(t, dir, files)

    for i := 0; i < fanout; i++ {
        child := filepath.Join(dir, fmt.Sprintf("dir_%d", i))
        total += buildLargeTree(t, child, depth-1, fanout)
    }
    return total
}

func TestDirectoryWalkerLargeTree(t *testing.T) {
    root := t.TempDir()
    expected := buildLargeTree(t, root, 3, 3) // 3 levels, branching factor 3, 2 files per directory

    walker := NewDirectoryWalker(nil)
    visited := 0
    walker.FileVisitor = func(name string, fm FileMetadata) error {
        visited++
        return nil
    }

    if err := walker.Walk(root); err != nil {
        t.Fatalf("walk failed: %v", err)
    }

    if visited != expected {
        t.Fatalf("visited %d files, expected %d", visited, expected)
    }
}
