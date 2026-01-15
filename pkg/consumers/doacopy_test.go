package consumers

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

// TestDoACopySuccess tests the successful copy case
func TestDoACopySuccess(t *testing.T) {
	// Setup directories
	srcDir := t.TempDir()
	destDir := t.TempDir()

	// Create source file with content
	srcFile := filepath.Join(srcDir, "test.txt")
	content := []byte("test file content")
	if err := os.WriteFile(srcFile, content, 0o644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	// Setup source directory metadata
	if err := RunCheckCalc([]string{srcDir}, CheckCalcOptions{CalcCount: 1}); err != nil {
		t.Fatalf("failed to run check calc on source: %v", err)
	}

	// Setup destination directory metadata (empty)
	if err := RunCheckCalc([]string{destDir}, CheckCalcOptions{CalcCount: 1}); err != nil {
		t.Fatalf("failed to run check calc on destination: %v", err)
	}

	// Call doACopy with real file copier
	fpath := core.NewFpath(srcFile)
	fs, err := doACopy(srcDir, destDir, "backup_vol1", fpath, nil) // nil uses default CopyFile

	if err != nil {
		t.Fatalf("doACopy failed: %v", err)
	}

	// Verify returned FileStruct has checksum
	if fs.Checksum == "" {
		t.Error("returned FileStruct should have checksum")
	}

	// Verify file was copied
	destFile := filepath.Join(destDir, "test.txt")
	if _, err := os.Stat(destFile); err != nil {
		t.Fatalf("destination file not created: %v", err)
	}

	// Verify destination file content matches
	copiedContent, err := os.ReadFile(destFile)
	if err != nil {
		t.Fatalf("failed to read copied file: %v", err)
	}
	if string(copiedContent) != string(content) {
		t.Errorf("content mismatch: expected %s, got %s", content, copiedContent)
	}

	// Verify source directory map was updated with tag
	dmSrc, err := core.DirectoryMapFromDir(core.Dirname(srcDir))
	if err != nil {
		t.Fatalf("failed to read source directory map: %v", err)
	}
	srcFS, ok := dmSrc.Get(core.Fname("test.txt"))
	if !ok {
		t.Fatal("source file not in directory map")
	}
	if !srcFS.HasTag("backup_vol1") {
		t.Error("source file missing backup tag")
	}

	// Verify destination directory map was updated
	dmDst, err := core.DirectoryMapFromDir(core.Dirname(destDir))
	if err != nil {
		t.Fatalf("failed to read destination directory map: %v", err)
	}
	dstFS, ok := dmDst.Get(core.Fname("test.txt"))
	if !ok {
		t.Fatal("destination file not in directory map")
	}
	if dstFS.Checksum != fs.Checksum {
		t.Errorf("destination checksum mismatch: expected %s, got %s", fs.Checksum, dstFS.Checksum)
	}
}

// TestDoACopyWithDummyCopy tests the ErrDummyCopy case
func TestDoACopyWithDummyCopy(t *testing.T) {
	srcDir := t.TempDir()
	destDir := t.TempDir()

	// Create source file
	srcFile := filepath.Join(srcDir, "test.txt")
	if err := os.WriteFile(srcFile, []byte("content"), 0o644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	// Setup metadata
	if err := RunCheckCalc([]string{srcDir, destDir}, CheckCalcOptions{CalcCount: 2}); err != nil {
		t.Logf("check calc warning: %v", err)
	}

	// Test copier that returns ErrDummyCopy
	dummyCopier := func(src, dst core.Fpath) error {
		return ErrDummyCopy
	}

	fpath := core.NewFpath(srcFile)
	fs, err := doACopy(srcDir, destDir, "backup_vol1", fpath, dummyCopier)

	if !errors.Is(err, ErrDummyCopy) {
		t.Fatalf("doACopy should return ErrDummyCopy, got: %v", err)
	}

	// Verify returned FileStruct is empty (dummy copy means no actual work)
	if fs.Checksum != "" {
		t.Error("dummy copy should return empty FileStruct")
	}

	// Verify destination file was NOT created
	destFile := filepath.Join(destDir, "test.txt")
	if _, err := os.Stat(destFile); !errors.Is(err, os.ErrNotExist) {
		t.Error("destination file should not exist for dummy copy")
	}
}

// TestDoACopyWithNoSpace tests the ErrNoSpace case
func TestDoACopyWithNoSpace(t *testing.T) {
	srcDir := t.TempDir()
	destDir := t.TempDir()

	// Create source file
	srcFile := filepath.Join(srcDir, "test.txt")
	if err := os.WriteFile(srcFile, []byte("content"), 0o644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	// Setup metadata
	if err := RunCheckCalc([]string{srcDir, destDir}, CheckCalcOptions{CalcCount: 2}); err != nil {
		t.Logf("check calc warning: %v", err)
	}

	// Test copier that returns ErrNoSpace
	noSpaceCopier := func(src, dst core.Fpath) error {
		return ErrNoSpace
	}

	fpath := core.NewFpath(srcFile)
	fs, err := doACopy(srcDir, destDir, "backup_vol1", fpath, noSpaceCopier)

	if !errors.Is(err, ErrNoSpace) {
		t.Errorf("expected ErrNoSpace, got %v", err)
	}

	// Verify returned FileStruct is empty on error
	if fs.Checksum != "" {
		t.Error("should return empty FileStruct on error")
	}
}

// TestDoACopyWithCustomError tests error handling for other errors
func TestDoACopyWithCustomError(t *testing.T) {
	srcDir := t.TempDir()
	destDir := t.TempDir()

	// Create source file
	srcFile := filepath.Join(srcDir, "test.txt")
	if err := os.WriteFile(srcFile, []byte("content"), 0o644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	// Setup metadata
	if err := RunCheckCalc([]string{srcDir, destDir}, CheckCalcOptions{CalcCount: 2}); err != nil {
		t.Logf("check calc warning: %v", err)
	}

	// Test copier that returns a custom error
	customErr := errors.New("custom copy error")
	copyCount := 0
	errorCopier := func(src, dst core.Fpath) error {
		copyCount++
		return customErr
	}

	fpath := core.NewFpath(srcFile)
	fs, err := doACopy(srcDir, destDir, "backup_vol1", fpath, errorCopier)

	// Should get an error (either the custom one or a file-related one from cleanup)
	if err == nil {
		t.Error("expected an error")
	}

	// Verify the copier was actually called
	if copyCount != 1 {
		t.Errorf("expected copier to be called once, was called %d times", copyCount)
	}

	// Verify returned FileStruct is empty on error
	if fs.Checksum != "" {
		t.Error("should return empty FileStruct on error")
	}
}

// TestDoACopyCopiesCorrectly tests that files are copied with proper hierarchy
func TestDoACopyCopiesCorrectly(t *testing.T) {
	srcDir := t.TempDir()
	destDir := t.TempDir()

	// Create nested directory structure in source
	nestedDir := filepath.Join(srcDir, "subdir")
	if err := os.MkdirAll(nestedDir, 0o755); err != nil {
		t.Fatalf("failed to create nested directory: %v", err)
	}

	// Create file in nested directory
	srcFile := filepath.Join(nestedDir, "nested.txt")
	content := []byte("nested file content")
	if err := os.WriteFile(srcFile, content, 0o644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	// Setup metadata
	if err := RunCheckCalc([]string{srcDir, destDir}, CheckCalcOptions{CalcCount: 2}); err != nil {
		t.Logf("check calc warning: %v", err)
	}

	// Call doACopy
	fpath := core.NewFpath(srcFile)
	fs, err := doACopy(srcDir, destDir, "backup_vol1", fpath, nil)

	if err != nil {
		t.Fatalf("doACopy failed: %v", err)
	}

	// Verify file was copied with correct hierarchy
	expectedDest := filepath.Join(destDir, "subdir", "nested.txt")
	copiedContent, err := os.ReadFile(expectedDest)
	if err != nil {
		t.Fatalf("destination file not created or not readable: %v", err)
	}

	if string(copiedContent) != string(content) {
		t.Errorf("content mismatch: expected %s, got %s", content, copiedContent)
	}

	if fs.Checksum == "" {
		t.Error("returned FileStruct should have checksum")
	}
}

// TestDoACopyMultipleTags tests that doACopy works with different volume labels
func TestDoACopyMultipleTags(t *testing.T) {
	srcDir := t.TempDir()
	destDir1 := t.TempDir()
	destDir2 := t.TempDir()

	// Create source file
	srcFile := filepath.Join(srcDir, "test.txt")
	if err := os.WriteFile(srcFile, []byte("content"), 0o644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	// Setup metadata
	if err := RunCheckCalc([]string{srcDir, destDir1, destDir2}, CheckCalcOptions{CalcCount: 3}); err != nil {
		t.Logf("check calc warning: %v", err)
	}

	// First copy to destination 1 with label "vol1"
	fpath := core.NewFpath(srcFile)
	_, err := doACopy(srcDir, destDir1, "vol1", fpath, nil)
	if err != nil {
		t.Fatalf("first doACopy failed: %v", err)
	}

	// Verify source has vol1 tag
	dmSrc, _ := core.DirectoryMapFromDir(core.Dirname(srcDir))
	srcFS, _ := dmSrc.Get(core.Fname("test.txt"))
	if !srcFS.HasTag("vol1") {
		t.Error("source file should have vol1 tag")
	}

	// Second copy to destination 2 with label "vol2" (same source file)
	_, err = doACopy(srcDir, destDir2, "vol2", fpath, nil)
	if err != nil {
		t.Fatalf("second doACopy failed: %v", err)
	}

	// Verify source now has both tags
	dmSrc, _ = core.DirectoryMapFromDir(core.Dirname(srcDir))
	srcFS, _ = dmSrc.Get(core.Fname("test.txt"))
	if !srcFS.HasTag("vol1") {
		t.Error("source file should still have vol1 tag")
	}
	if !srcFS.HasTag("vol2") {
		t.Error("source file should have vol2 tag")
	}
}
