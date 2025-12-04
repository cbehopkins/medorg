package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

// TestCopyFileImpl_SourceDoesNotExist tests error when source file doesn't exist
func TestCopyFileImpl_SourceDoesNotExist(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "nonexistent.txt")
	dst := filepath.Join(dir, "dst.txt")

	err := copyFileImpl(src, dst)
	if err == nil {
		t.Error("Expected error when source file doesn't exist")
	}
}

// TestCopyFileImpl_SourceChecksumFail tests error when source checksum calculation fails
func TestCopyFileImpl_SourceChecksumFail(t *testing.T) {
	dir := t.TempDir()

	// Create a directory where we expect a file (to cause checksum to fail)
	src := filepath.Join(dir, "srcdir")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}

	dst := filepath.Join(dir, "dst.txt")

	err := copyFileImpl(src, dst)
	if err == nil {
		t.Error("Expected error when checksumming directory as file")
	}
}

// TestCopyFileImpl_DestinationDirCreation tests that destination directory is created
func TestCopyFileImpl_DestinationDirCreation(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "nested", "deep", "dst.txt")

	if err := os.WriteFile(src, []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}

	err := copyFileImpl(src, dst)
	if err != nil {
		t.Fatalf("copyFileImpl failed: %v", err)
	}

	// Verify file was created
	data, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("Failed to read destination: %v", err)
	}
	if string(data) != "content" {
		t.Errorf("Content mismatch: got %q, want %q", string(data), "content")
	}

	// Verify checksums match
	sdir, sbase := filepath.Split(src)
	ddir, dbase := filepath.Split(dst)

	sh, err := core.CalcMd5File(sdir, sbase)
	if err != nil {
		t.Fatalf("Failed to checksum source: %v", err)
	}

	dh, err := core.CalcMd5File(ddir, dbase)
	if err != nil {
		t.Fatalf("Failed to checksum destination: %v", err)
	}

	if sh != dh {
		t.Errorf("Checksum mismatch: %s vs %s", sh, dh)
	}
}

// TestCopyFileImpl_OverwriteExisting tests overwriting existing destination
func TestCopyFileImpl_OverwriteExisting(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")

	if err := os.WriteFile(src, []byte("new"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Create existing destination with different content
	if err := os.WriteFile(dst, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}

	err := copyFileImpl(src, dst)
	if err != nil {
		t.Fatalf("copyFileImpl failed: %v", err)
	}

	// Verify file was overwritten
	data, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("Failed to read destination: %v", err)
	}
	if string(data) != "new" {
		t.Errorf("Content should be 'new', got %q", string(data))
	}
}
