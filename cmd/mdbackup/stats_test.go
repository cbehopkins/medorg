package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

// Unit tests for runStatsSimple function

// TestRunStatsSimpleEmptyDirectory tests stats with no files
func TestRunStatsSimpleEmptyDirectory(t *testing.T) {
	tmpDir := t.TempDir()

	var msgBuf, logBuf bytes.Buffer
	setMessage := func(msg string) {
		msgBuf.WriteString(msg + "\n")
	}
	logFunc := func(msg string) {
		logBuf.WriteString(msg + "\n")
	}

	runStatsSimple([]string{tmpDir}, setMessage, logFunc)

	output := logBuf.String()
	if !strings.Contains(output, "requires") {
		t.Errorf("Expected stats output, got: %s", output)
	}
}

// TestRunStatsSimpleSingleFile tests stats with one file
func TestRunStatsSimpleSingleFile(t *testing.T) {
	tmpDir := t.TempDir()

	// Create config and file with metadata
	xc, err := core.NewMdConfig(filepath.Join(tmpDir, ".medorg.xml"))
	if err != nil {
		t.Fatalf("Failed to create config: %v", err)
	}
	defer xc.WriteXmlCfg()

	// Create a test file
	testFile := filepath.Join(tmpDir, "test.txt")
	content := []byte("test content")
	if err := os.WriteFile(testFile, content, 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	var msgBuf, logBuf bytes.Buffer
	setMessage := func(msg string) {
		msgBuf.WriteString(msg + "\n")
	}
	logFunc := func(msg string) {
		logBuf.WriteString(msg + "\n")
	}

	runStatsSimple([]string{tmpDir}, setMessage, logFunc)

	output := logBuf.String()
	if !strings.Contains(output, "requires") {
		t.Errorf("Expected stats output, got: %s", output)
	}
}

// TestRunStatsSimpleMultipleFiles tests stats with multiple files in different states
func TestRunStatsSimpleMultipleFiles(t *testing.T) {
	tmpDir := t.TempDir()

	// Create files
	files := []string{"file1.txt", "file2.txt", "file3.txt"}
	for _, f := range files {
		fullPath := filepath.Join(tmpDir, f)
		if err := os.WriteFile(fullPath, []byte("content"), 0o644); err != nil {
			t.Fatalf("Failed to create file %s: %v", f, err)
		}
	}

	var msgBuf, logBuf bytes.Buffer
	setMessage := func(msg string) {
		msgBuf.WriteString(msg + "\n")
	}
	logFunc := func(msg string) {
		logBuf.WriteString(msg + "\n")
	}

	runStatsSimple([]string{tmpDir}, setMessage, logFunc)

	output := logBuf.String()
	if !strings.Contains(output, "0 requires") {
		t.Errorf("Expected '0 requires' in stats for unbackedup files, got: %s", output)
	}
}

// TestRunStatsSimpleMultipleDirectories tests stats with multiple source directories
func TestRunStatsSimpleMultipleDirectories(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	// Create files in each directory
	if err := os.WriteFile(filepath.Join(dir1, "file1.txt"), []byte("content1"), 0o644); err != nil {
		t.Fatalf("Failed to create file in dir1: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir2, "file2.txt"), []byte("content2"), 0o644); err != nil {
		t.Fatalf("Failed to create file in dir2: %v", err)
	}

	var msgBuf, logBuf bytes.Buffer
	setMessage := func(msg string) {
		msgBuf.WriteString(msg + "\n")
	}
	logFunc := func(msg string) {
		logBuf.WriteString(msg + "\n")
	}

	runStatsSimple([]string{dir1, dir2}, setMessage, logFunc)

	output := logBuf.String()
	if !strings.Contains(output, "requires") {
		t.Errorf("Expected stats output for multiple directories, got: %s", output)
	}
}

// TestRunStatsSimpleMessageOutput tests that stats produce proper message output
func TestRunStatsSimpleMessageOutput(t *testing.T) {
	tmpDir := t.TempDir()

	if err := os.WriteFile(filepath.Join(tmpDir, "test.txt"), []byte("test"), 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	var msgBuf bytes.Buffer
	setMessage := func(msg string) {
		msgBuf.WriteString(msg + "\n")
	}
	logFunc := func(msg string) {}

	runStatsSimple([]string{tmpDir}, setMessage, logFunc)

	output := msgBuf.String()
	if !strings.Contains(output, "Start Scanning") {
		t.Errorf("Expected 'Start Scanning' message, got: %s", output)
	}
}

// TestRunStatsSimpleSubdirectories tests stats with nested directories
func TestRunStatsSimpleSubdirectories(t *testing.T) {
	tmpDir := t.TempDir()

	// Create nested structure
	subdir := filepath.Join(tmpDir, "subdir")
	if err := os.Mkdir(subdir, 0o755); err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}

	if err := os.WriteFile(filepath.Join(tmpDir, "root.txt"), []byte("root"), 0o644); err != nil {
		t.Fatalf("Failed to create root file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(subdir, "nested.txt"), []byte("nested"), 0o644); err != nil {
		t.Fatalf("Failed to create nested file: %v", err)
	}

	var msgBuf, logBuf bytes.Buffer
	setMessage := func(msg string) {
		msgBuf.WriteString(msg + "\n")
	}
	logFunc := func(msg string) {
		logBuf.WriteString(msg + "\n")
	}

	runStatsSimple([]string{tmpDir}, setMessage, logFunc)

	output := logBuf.String()
	if !strings.Contains(output, "requires") {
		t.Errorf("Expected stats output for nested directories, got: %s", output)
	}
}

// TestRunStatsSimpleOutputFormat tests that stats output has correct format
func TestRunStatsSimpleOutputFormat(t *testing.T) {
	tmpDir := t.TempDir()

	if err := os.WriteFile(filepath.Join(tmpDir, "test.txt"), []byte("test"), 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	var logBuf bytes.Buffer
	setMessage := func(msg string) {}
	logFunc := func(msg string) {
		logBuf.WriteString(msg + "\n")
	}

	runStatsSimple([]string{tmpDir}, setMessage, logFunc)

	output := logBuf.String()
	// Should have "N requires X bytes" format
	if !strings.Contains(output, "requires") || !strings.Contains(output, "bytes") {
		t.Errorf("Expected 'N requires X bytes' format, got: %s", output)
	}
}

// TestRunStatsSimpleLargeFiles tests stats with large files
func TestRunStatsSimpleLargeFiles(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a large file (just truncate it for speed)
	largeFile := filepath.Join(tmpDir, "large.bin")
	if err := os.Truncate(largeFile, 1024*1024); err != nil { // 1MB
		t.Skipf("Cannot create large sparse file: %v", err)
	}

	var logBuf bytes.Buffer
	setMessage := func(msg string) {}
	logFunc := func(msg string) {
		logBuf.WriteString(msg + "\n")
	}

	runStatsSimple([]string{tmpDir}, setMessage, logFunc)

	output := logBuf.String()
	if !strings.Contains(output, "requires") {
		t.Errorf("Expected stats output for large files, got: %s", output)
	}
}
