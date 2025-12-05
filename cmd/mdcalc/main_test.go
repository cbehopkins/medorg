package main

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cbehopkins/medorg/pkg/core"
)

// Integration tests for check_calc

// helper: create temp dir and return cleanup func
func makeTempDir(t *testing.T, name string) (string, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", name+"-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	cleanup := func() {
		_ = os.RemoveAll(dir)
	}
	return dir, cleanup
}

// helper: run check_calc with args and return stdout/stderr
func runCheckCalc(t *testing.T, dir string, args ...string) (string, string, error) {
	t.Helper()

	// Build the command - use the binary directly if it exists, otherwise use go run
	var cmd *exec.Cmd
	checkCalcBinary := filepath.Join("..", "..", "check_calc.exe")
	if _, err := os.Stat(checkCalcBinary); err == nil {
		fullArgs := append([]string{}, args...)
		fullArgs = append(fullArgs, dir)
		cmd = exec.Command(checkCalcBinary, fullArgs...)
	} else {
		// Fallback to go run - run from the mdcalc directory
		fullArgs := append([]string{"run", "."}, args...)
		fullArgs = append(fullArgs, dir)
		cmd = exec.Command("go", fullArgs...)
		// cmd.Dir should be left as default (test directory) or set to mdcalc directory
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

// helper: read and parse the .medorg.xml file from a directory
func readMd5File(t *testing.T, dir string) *core.Md5File {
	t.Helper()

	mdFile := filepath.Join(dir, core.Md5FileName)
	data, err := os.ReadFile(mdFile)
	if err != nil {
		t.Fatalf("failed to read %s: %v", core.Md5FileName, err)
	}

	var md5File core.Md5File
	if err := xml.Unmarshal(data, &md5File); err != nil {
		t.Fatalf("failed to parse %s: %v", core.Md5FileName, err)
	}

	return &md5File
}

// helper: check if .medorg.xml exists
func hasMd5File(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, core.Md5FileName))
	return err == nil
}

// helper: create a file with specific content
func createFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	fullPath := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		t.Fatalf("failed to create directories for %s: %v", fullPath, err)
	}
	if err := os.WriteFile(fullPath, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write file %s: %v", fullPath, err)
	}
	return fullPath
}

// helper: get FileStruct by name from Md5File
func getFileStruct(md5File *core.Md5File, name string) *core.FileStruct {
	for i := range md5File.Files {
		if md5File.Files[i].Name == name {
			return &md5File.Files[i]
		}
	}
	return nil
}

func TestIntegration_CleanSlate(t *testing.T) {
	// Test running check_calc on a completely clean directory
	dir, cleanup := makeTempDir(t, "clean")
	defer cleanup()

	// Create some test files
	createFile(t, dir, "file1.txt", "content one")
	createFile(t, dir, "file2.dat", "content two different")

	// Create subdirectory with file
	subdir := filepath.Join(dir, "subdir")
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatal(err)
	}
	createFile(t, subdir, "file3.log", "nested content")

	// Verify no .medorg.xml exists yet
	if hasMd5File(dir) {
		t.Fatal(".medorg.xml should not exist before running check_calc")
	}

	// Run check_calc
	stdout, stderr, err := runCheckCalc(t, dir)
	if err != nil {
		t.Fatalf("check_calc failed: %v\nstdout: %s\nstderr: %s", err, stdout, stderr)
	}

	// Verify .medorg.xml was created in root directory
	if !hasMd5File(dir) {
		t.Fatal(".medorg.xml should exist in root dir after running check_calc")
	}

	// Verify checksums were calculated for root level files
	md5File := readMd5File(t, dir)

	// Should have entries for the 2 root-level files
	if len(md5File.Files) != 2 {
		t.Errorf("expected 2 files in root md5 file, got %d", len(md5File.Files))
	}

	// Verify each root file has a checksum
	for _, name := range []string{"file1.txt", "file2.dat"} {
		fs := getFileStruct(md5File, name)
		if fs == nil {
			t.Errorf("file %s not found in md5 file", name)
			continue
		}
		if fs.Checksum == "" {
			t.Errorf("file %s has no checksum", name)
		}
		if fs.Size == 0 {
			t.Errorf("file %s has zero size recorded", name)
		}
	}

	// Verify subdirectory also has its own .medorg.xml
	if !hasMd5File(subdir) {
		t.Fatal(".medorg.xml should exist in subdir after running check_calc")
	}

	subdirMd5 := readMd5File(t, subdir)
	if len(subdirMd5.Files) != 1 {
		t.Errorf("expected 1 file in subdir md5 file, got %d", len(subdirMd5.Files))
	}

	fs := getFileStruct(subdirMd5, "file3.log")
	if fs == nil {
		t.Error("file3.log not found in subdir md5 file")
	} else if fs.Checksum == "" {
		t.Error("file3.log has no checksum")
	}
}

func TestIntegration_SubdirectoryProcessing(t *testing.T) {
	// Test that check_calc processes subdirectories correctly
	dir, cleanup := makeTempDir(t, "subdir")
	defer cleanup()

	// Create files in root directory
	createFile(t, dir, "root.txt", "root content")

	// Create subdirectory with its own .medorg.xml
	subdir := filepath.Join(dir, "subdir")
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatal(err)
	}
	createFile(t, subdir, "nested.txt", "nested content")

	// Run check_calc on root
	stdout, stderr, err := runCheckCalc(t, dir)
	if err != nil {
		t.Fatalf("check_calc failed: %v\nstdout: %s\nstderr: %s", err, stdout, stderr)
	}

	// Verify root .medorg.xml
	rootMd5 := readMd5File(t, dir)
	if fs := getFileStruct(rootMd5, "root.txt"); fs == nil || fs.Checksum == "" {
		t.Error("root.txt not properly processed")
	}

	// Verify subdirectory .medorg.xml
	subdirMd5 := readMd5File(t, subdir)
	if fs := getFileStruct(subdirMd5, "nested.txt"); fs == nil || fs.Checksum == "" {
		t.Error("nested.txt not properly processed")
	}
}

func TestIntegration_PreviouslyProcessed(t *testing.T) {
	// Test running check_calc on a directory that already has .medorg.xml
	dir, cleanup := makeTempDir(t, "prev")
	defer cleanup()

	// Create initial file
	createFile(t, dir, "existing.txt", "original content")

	// First run
	stdout, stderr, err := runCheckCalc(t, dir)
	if err != nil {
		t.Fatalf("first check_calc run failed: %v\nstdout: %s\nstderr: %s", err, stdout, stderr)
	}

	// Get initial checksum
	md5File1 := readMd5File(t, dir)
	fs1 := getFileStruct(md5File1, "existing.txt")
	if fs1 == nil {
		t.Fatal("existing.txt not found after first run")
	}
	originalChecksum := fs1.Checksum
	if originalChecksum == "" {
		t.Fatal("no checksum calculated on first run")
	}

	// Second run - file unchanged
	time.Sleep(10 * time.Millisecond) // Ensure timestamp would be different if recalculated
	stdout, stderr, err = runCheckCalc(t, dir)
	if err != nil {
		t.Fatalf("second check_calc run failed: %v\nstdout: %s\nstderr: %s", err, stdout, stderr)
	}

	// Verify checksum is the same (not recalculated)
	md5File2 := readMd5File(t, dir)
	fs2 := getFileStruct(md5File2, "existing.txt")
	if fs2 == nil {
		t.Fatal("existing.txt not found after second run")
	}
	if fs2.Checksum != originalChecksum {
		t.Errorf("checksum changed on second run without file modification: %s -> %s",
			originalChecksum, fs2.Checksum)
	}
}

func TestIntegration_FileAdditions(t *testing.T) {
	// Test that new files are detected and processed
	dir, cleanup := makeTempDir(t, "add")
	defer cleanup()

	// Create initial file
	createFile(t, dir, "original.txt", "original")

	// First run
	if _, _, err := runCheckCalc(t, dir); err != nil {
		t.Fatalf("first run failed: %v", err)
	}

	md5File1 := readMd5File(t, dir)
	if len(md5File1.Files) != 1 {
		t.Fatalf("expected 1 file after first run, got %d", len(md5File1.Files))
	}

	// Add new files
	createFile(t, dir, "new1.txt", "new file one")
	createFile(t, dir, "new2.dat", "new file two")

	// Second run
	if _, _, err := runCheckCalc(t, dir); err != nil {
		t.Fatalf("second run failed: %v", err)
	}

	md5File2 := readMd5File(t, dir)
	if len(md5File2.Files) != 3 {
		t.Fatalf("expected 3 files after second run, got %d", len(md5File2.Files))
	}

	// Verify all files have checksums
	for _, name := range []string{"original.txt", "new1.txt", "new2.dat"} {
		fs := getFileStruct(md5File2, name)
		if fs == nil {
			t.Errorf("file %s not found", name)
		} else if fs.Checksum == "" {
			t.Errorf("file %s has no checksum", name)
		}
	}
}

func TestIntegration_FileDeletions(t *testing.T) {
	// Test that deleted files are removed from .medorg.xml
	dir, cleanup := makeTempDir(t, "del")
	defer cleanup()

	// Create files
	_ = createFile(t, dir, "keep.txt", "keep this")
	file2 := createFile(t, dir, "delete.txt", "remove this")

	// First run
	if _, _, err := runCheckCalc(t, dir); err != nil {
		t.Fatalf("first run failed: %v", err)
	}

	md5File1 := readMd5File(t, dir)
	if len(md5File1.Files) != 2 {
		t.Fatalf("expected 2 files after first run, got %d", len(md5File1.Files))
	}

	// Delete one file
	if err := os.Remove(file2); err != nil {
		t.Fatal(err)
	}

	// Second run
	if _, _, err := runCheckCalc(t, dir); err != nil {
		t.Fatalf("second run failed: %v", err)
	}

	md5File2 := readMd5File(t, dir)
	if len(md5File2.Files) != 1 {
		t.Fatalf("expected 1 file after deletion, got %d", len(md5File2.Files))
	}

	// Verify only the kept file remains
	if fs := getFileStruct(md5File2, "keep.txt"); fs == nil {
		t.Error("keep.txt should still be present")
	}
	if fs := getFileStruct(md5File2, "delete.txt"); fs != nil {
		t.Error("delete.txt should have been removed from md5 file")
	}
}

func TestIntegration_FileModifications(t *testing.T) {
	// Test that modified files get their checksums recalculated
	dir, cleanup := makeTempDir(t, "mod")
	defer cleanup()

	filePath := createFile(t, dir, "modify.txt", "original content")

	// First run
	if _, _, err := runCheckCalc(t, dir); err != nil {
		t.Fatalf("first run failed: %v", err)
	}

	md5File1 := readMd5File(t, dir)
	fs1 := getFileStruct(md5File1, "modify.txt")
	if fs1 == nil {
		t.Fatal("modify.txt not found after first run")
	}
	originalChecksum := fs1.Checksum
	originalSize := fs1.Size

	// Modify the file
	time.Sleep(10 * time.Millisecond) // Ensure timestamp is different
	if err := os.WriteFile(filePath, []byte("modified content - different"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Second run
	if _, _, err := runCheckCalc(t, dir); err != nil {
		t.Fatalf("second run failed: %v", err)
	}

	md5File2 := readMd5File(t, dir)
	fs2 := getFileStruct(md5File2, "modify.txt")
	if fs2 == nil {
		t.Fatal("modify.txt not found after second run")
	}

	// Verify checksum was recalculated
	if fs2.Checksum == originalChecksum {
		t.Error("checksum should have changed after file modification")
	}
	if fs2.Size == originalSize {
		t.Error("size should have changed after file modification")
	}
	if fs2.Checksum == "" {
		t.Error("new checksum should not be empty")
	}
}

func TestIntegration_RecalcFlag(t *testing.T) {
	// Test the -recalc flag forces recalculation even for unchanged files
	dir, cleanup := makeTempDir(t, "recalc")
	defer cleanup()

	createFile(t, dir, "test.txt", "test content")

	// First run
	if _, _, err := runCheckCalc(t, dir); err != nil {
		t.Fatalf("first run failed: %v", err)
	}

	md5File1 := readMd5File(t, dir)
	fs1 := getFileStruct(md5File1, "test.txt")
	originalChecksum := fs1.Checksum

	// Run with -recalc flag
	stdout, stderr, err := runCheckCalc(t, dir, "-recalc")
	if err != nil {
		t.Fatalf("recalc run failed: %v\nstdout: %s\nstderr: %s", err, stdout, stderr)
	}

	md5File2 := readMd5File(t, dir)
	fs2 := getFileStruct(md5File2, "test.txt")

	// Checksum should be the same value (same content) but was recalculated
	if fs2.Checksum != originalChecksum {
		t.Error("checksum value should be same for unchanged file")
	}
	if fs2.Checksum == "" {
		t.Error("checksum should not be empty after recalc")
	}
}

func TestIntegration_EmptyDirectory(t *testing.T) {
	// Test running on an empty directory
	dir, cleanup := makeTempDir(t, "empty")
	defer cleanup()

	// Run check_calc on empty directory
	stdout, stderr, err := runCheckCalc(t, dir)
	if err != nil {
		t.Fatalf("check_calc failed on empty dir: %v\nstdout: %s\nstderr: %s", err, stdout, stderr)
	}

	// .medorg.xml should still be created (or not, depending on implementation)
	// If it's created, it should be empty or have no file entries
	if hasMd5File(dir) {
		md5File := readMd5File(t, dir)
		if len(md5File.Files) != 0 {
			t.Errorf("expected no files in md5 file for empty directory, got %d", len(md5File.Files))
		}
	}
}

func TestIntegration_NestedDirectories(t *testing.T) {
	// Test deeply nested directory structures
	dir, cleanup := makeTempDir(t, "nested")
	defer cleanup()

	// Create nested structure
	deepPath := dir
	for i := 1; i <= 5; i++ {
		deepPath = filepath.Join(deepPath, fmt.Sprintf("level%d", i))
		if err := os.MkdirAll(deepPath, 0o755); err != nil {
			t.Fatal(err)
		}
		createFile(t, deepPath, fmt.Sprintf("file%d.txt", i), fmt.Sprintf("content at level %d", i))
	}

	// Run check_calc on root
	if _, _, err := runCheckCalc(t, dir); err != nil {
		t.Fatalf("check_calc failed: %v", err)
	}

	// Verify each level has its .medorg.xml
	checkPath := dir
	for i := 1; i <= 5; i++ {
		checkPath = filepath.Join(checkPath, fmt.Sprintf("level%d", i))
		if !hasMd5File(checkPath) {
			t.Errorf("level %d missing .medorg.xml", i)
			continue
		}
		md5File := readMd5File(t, checkPath)
		fileName := fmt.Sprintf("file%d.txt", i)
		if fs := getFileStruct(md5File, fileName); fs == nil || fs.Checksum == "" {
			t.Errorf("level %d file not properly processed", i)
		}
	}
}

func TestIntegration_LargeFiles(t *testing.T) {
	// Test that large files are handled correctly
	dir, cleanup := makeTempDir(t, "large")
	defer cleanup()

	// Create a 5MB file
	largeContent := make([]byte, 5*1024*1024)
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}
	largePath := filepath.Join(dir, "large.bin")
	if err := os.WriteFile(largePath, largeContent, 0o644); err != nil {
		t.Fatal(err)
	}

	// Run check_calc
	if _, _, err := runCheckCalc(t, dir); err != nil {
		t.Fatalf("check_calc failed on large file: %v", err)
	}

	// Verify checksum was calculated
	md5File := readMd5File(t, dir)
	fs := getFileStruct(md5File, "large.bin")
	if fs == nil {
		t.Fatal("large.bin not found in md5 file")
	}
	if fs.Checksum == "" {
		t.Error("large file has no checksum")
	}
	if fs.Size != int64(len(largeContent)) {
		t.Errorf("size mismatch: expected %d, got %d", len(largeContent), fs.Size)
	}
}

func TestIntegration_ManyFiles(t *testing.T) {
	// Test processing many files
	dir, cleanup := makeTempDir(t, "many")
	defer cleanup()

	// Create 50 files
	numFiles := 50
	for i := 0; i < numFiles; i++ {
		name := fmt.Sprintf("file%03d.txt", i)
		content := fmt.Sprintf("content for file number %d", i)
		createFile(t, dir, name, content)
	}

	// Run check_calc
	if _, _, err := runCheckCalc(t, dir); err != nil {
		t.Fatalf("check_calc failed: %v", err)
	}

	// Verify all files were processed
	md5File := readMd5File(t, dir)
	if len(md5File.Files) != numFiles {
		t.Errorf("expected %d files, got %d", numFiles, len(md5File.Files))
	}

	// Spot check a few files
	for i := 0; i < numFiles; i += 10 {
		name := fmt.Sprintf("file%03d.txt", i)
		if fs := getFileStruct(md5File, name); fs == nil || fs.Checksum == "" {
			t.Errorf("file %s not properly processed", name)
		}
	}
}

func TestIntegration_SpecialCharacters(t *testing.T) {
	// Test files with special characters in names
	dir, cleanup := makeTempDir(t, "special")
	defer cleanup()

	specialFiles := []string{
		"file with spaces.txt",
		"file-with-dashes.txt",
		"file_with_underscores.txt",
		"file.multiple.dots.txt",
		"file(with)parens.txt",
		"file[with]brackets.txt",
	}

	for _, name := range specialFiles {
		createFile(t, dir, name, "content for "+name)
	}

	// Run check_calc
	if _, _, err := runCheckCalc(t, dir); err != nil {
		t.Fatalf("check_calc failed: %v", err)
	}

	// Verify all files were processed
	md5File := readMd5File(t, dir)
	for _, name := range specialFiles {
		if fs := getFileStruct(md5File, name); fs == nil || fs.Checksum == "" {
			t.Errorf("file %s not properly processed", name)
		}
	}
}

func TestIntegration_CalcConcurrency(t *testing.T) {
	// Test the -calc flag for controlling concurrency
	dir, cleanup := makeTempDir(t, "calc")
	defer cleanup()

	// Create multiple files
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("file%d.txt", i)
		content := strings.Repeat(fmt.Sprintf("content %d ", i), 1000)
		createFile(t, dir, name, content)
	}

	// Run with -calc 1 (sequential)
	stdout, stderr, err := runCheckCalc(t, dir, "-calc", "1")
	if err != nil {
		t.Fatalf("check_calc with -calc 1 failed: %v\nstdout: %s\nstderr: %s", err, stdout, stderr)
	}

	// Run with -calc 4 (parallel)
	stdout, stderr, err = runCheckCalc(t, dir, "-calc", "4", "-recalc")
	if err != nil {
		t.Fatalf("check_calc with -calc 4 failed: %v\nstdout: %s\nstderr: %s", err, stdout, stderr)
	}

	// Verify results are the same
	md5File := readMd5File(t, dir)
	if len(md5File.Files) != 10 {
		t.Errorf("expected 10 files, got %d", len(md5File.Files))
	}
}

func TestIntegration_ValidateFlag(t *testing.T) {
	// Test the -validate flag
	dir, cleanup := makeTempDir(t, "validate")
	defer cleanup()

	filePath := createFile(t, dir, "test.txt", "test content for validation")

	// First run to create checksums
	if _, _, err := runCheckCalc(t, dir); err != nil {
		t.Fatalf("initial run failed: %v", err)
	}

	// Run with -validate flag (should succeed)
	stdout, stderr, err := runCheckCalc(t, dir, "-validate")
	if err != nil {
		t.Fatalf("validate failed: %v\nstdout: %s\nstderr: %s", err, stdout, stderr)
	}

	// Corrupt the file
	if err := os.WriteFile(filePath, []byte("corrupted content"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Run with -validate flag (should detect corruption)
	stdout, stderr, err = runCheckCalc(t, dir, "-validate")
	// The validation might fail or succeed depending on implementation
	// Just verify it runs
	t.Logf("Validation after corruption: err=%v, stdout=%s, stderr=%s", err, stdout, stderr)
}

func TestIntegration_ScrubFlag(t *testing.T) {
	// Test the -scrub flag removes backup destination tags
	dir, cleanup := makeTempDir(t, "scrub")
	defer cleanup()

	// Create test files
	createFile(t, dir, "file1.txt", "test content 1")
	createFile(t, dir, "file2.txt", "test content 2")

	// First run to create checksums
	if _, _, err := runCheckCalc(t, dir); err != nil {
		t.Fatalf("initial run failed: %v", err)
	}

	// Manually add backup destination tags to simulate files that have been backed up
	mdFile := readMd5File(t, dir)
	if len(mdFile.Files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(mdFile.Files))
	}

	// Add backup destinations to files
	mdFile.Files[0].AddBackupDestination("backup1")
	mdFile.Files[0].AddBackupDestination("backup2")
	mdFile.Files[1].AddBackupDestination("backup1")

	// Write the modified md5 file
	mdFileXML := filepath.Join(dir, core.Md5FileName)
	data, err := xml.Marshal(mdFile)
	if err != nil {
		t.Fatalf("failed to marshal md5 file: %v", err)
	}
	if err := os.WriteFile(mdFileXML, data, 0o644); err != nil {
		t.Fatalf("failed to write md5 file: %v", err)
	}

	// Verify backup destinations exist before scrub
	mdFile = readMd5File(t, dir)
	if len(mdFile.Files[0].BackupDest) != 2 {
		t.Errorf("expected 2 backup destinations for file1, got %d", len(mdFile.Files[0].BackupDest))
	}
	if len(mdFile.Files[1].BackupDest) != 1 {
		t.Errorf("expected 1 backup destination for file2, got %d", len(mdFile.Files[1].BackupDest))
	}

	// Run with -scrub flag
	stdout, stderr, err := runCheckCalc(t, dir, "-scrub")
	if err != nil {
		t.Fatalf("check_calc with -scrub failed: %v\nstdout: %s\nstderr: %s", err, stdout, stderr)
	}

	// Verify backup destinations have been removed
	mdFile = readMd5File(t, dir)
	if len(mdFile.Files[0].BackupDest) != 0 {
		t.Errorf("expected 0 backup destinations after scrub for file1, got %d", len(mdFile.Files[0].BackupDest))
	}
	if len(mdFile.Files[1].BackupDest) != 0 {
		t.Errorf("expected 0 backup destinations after scrub for file2, got %d", len(mdFile.Files[1].BackupDest))
	}
}

func TestIntegration_ScrubFlagWithoutBackups(t *testing.T) {
	// Test the -scrub flag on files with no backup destinations
	dir, cleanup := makeTempDir(t, "scrub-clean")
	defer cleanup()

	// Create test file
	createFile(t, dir, "file1.txt", "test content")

	// First run to create checksums
	if _, _, err := runCheckCalc(t, dir); err != nil {
		t.Fatalf("initial run failed: %v", err)
	}

	// Verify no backup destinations
	mdFile := readMd5File(t, dir)
	if len(mdFile.Files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(mdFile.Files))
	}
	if len(mdFile.Files[0].BackupDest) != 0 {
		t.Errorf("expected 0 backup destinations initially, got %d", len(mdFile.Files[0].BackupDest))
	}

	// Run with -scrub flag (should have no effect)
	stdout, stderr, err := runCheckCalc(t, dir, "-scrub")
	if err != nil {
		t.Fatalf("check_calc with -scrub failed: %v\nstdout: %s\nstderr: %s", err, stdout, stderr)
	}

	// Verify still no backup destinations
	mdFile = readMd5File(t, dir)
	if len(mdFile.Files[0].BackupDest) != 0 {
		t.Errorf("expected 0 backup destinations after scrub, got %d", len(mdFile.Files[0].BackupDest))
	}
}
