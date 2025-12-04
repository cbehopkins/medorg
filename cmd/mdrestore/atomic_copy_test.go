package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

func TestCopyFile_AtomicChecksumAndOverwrite(t *testing.T) {
	dir := t.TempDir()
	srcDir := filepath.Join(dir, "src")
	dstDir := filepath.Join(dir, "dst")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatalf("mkdir src: %v", err)
	}
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		t.Fatalf("mkdir dst: %v", err)
	}

	// Create a source file
	src := filepath.Join(srcDir, "a", "b", "file.txt")
	if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
		t.Fatalf("mkdir src parents: %v", err)
	}
	srcData := []byte("hello world")
	if err := os.WriteFile(src, srcData, 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}

	// Destination path (nested)
	dst := filepath.Join(dstDir, "x", "y", "file.txt")

	// 1) Copy when destination doesn't exist
	if err := copyFile(src, dst); err != nil {
		t.Fatalf("copyFile failed: %v", err)
	}

	// Verify checksum match
	sdir, sbase := filepath.Split(src)
	ddir, dbase := filepath.Split(dst)
	sh, err := core.CalcMd5File(sdir, sbase)
	if err != nil {
		t.Fatalf("src checksum: %v", err)
	}
	dh, err := core.CalcMd5File(ddir, dbase)
	if err != nil {
		t.Fatalf("dst checksum: %v", err)
	}
	if sh != dh {
		t.Fatalf("checksum mismatch after copy: %s vs %s", sh, dh)
	}

	// Additional sanity: second copy should succeed and preserve checksum
	if err := copyFile(src, dst); err != nil {
		t.Fatalf("copyFile second copy failed: %v", err)
	}
	dh2, err := core.CalcMd5File(ddir, dbase)
	if err != nil { t.Fatalf("dst checksum after second copy: %v", err) }
	if sh != dh2 {
		t.Fatalf("checksum mismatch after second copy: %s vs %s", sh, dh2)
	}
}
