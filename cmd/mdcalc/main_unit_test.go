package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/medorg/pkg/adaptive"
	"github.com/cbehopkins/medorg/pkg/consumers"
	"github.com/cbehopkins/medorg/pkg/core"
)

// Unit tests for mdcalc main logic (without using exec)
// These tests directly invoke the functions to measure code coverage

// TestCheckCalcBasicRun tests basic checksum calculation
func TestCheckCalcBasicRun(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test files
	if err := os.WriteFile(filepath.Join(tmpDir, "file1.txt"), []byte("content1"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "file2.txt"), []byte("content2"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Run basic checksum calculation
	opts := consumers.CheckCalcOptions{
		CalcCount:    1,
		Recalc:       false,
		Validate:     false,
		Scrub:        false,
		ShowProgress: false,
		AutoFix:      nil,
		Tuner:        nil,
	}

	err := consumers.RunCheckCalc([]string{tmpDir}, opts)
	if err != nil {
		t.Fatalf("RunCheckCalc failed: %v", err)
	}

	// Verify .medorg.xml was created
	mdFile := filepath.Join(tmpDir, core.Md5FileName)
	if _, err := os.Stat(mdFile); os.IsNotExist(err) {
		t.Errorf("Expected %s to be created", core.Md5FileName)
	}
}

// TestCheckCalcWithRecalc tests recalculation flag
func TestCheckCalcWithRecalc(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test file
	testFile := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("original"), 0o644); err != nil {
		t.Fatal(err)
	}

	// First run
	opts := consumers.CheckCalcOptions{
		CalcCount:    1,
		Recalc:       false,
		Validate:     false,
		Scrub:        false,
		ShowProgress: false,
		AutoFix:      nil,
		Tuner:        nil,
	}

	if err := consumers.RunCheckCalc([]string{tmpDir}, opts); err != nil {
		t.Fatalf("First RunCheckCalc failed: %v", err)
	}

	// Modify file
	if err := os.WriteFile(testFile, []byte("modified"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Second run with recalc=true
	opts.Recalc = true
	if err := consumers.RunCheckCalc([]string{tmpDir}, opts); err != nil {
		t.Fatalf("Second RunCheckCalc failed: %v", err)
	}
}

// TestCheckCalcWithValidate tests validation flag
func TestCheckCalcWithValidate(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test file
	testFile := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("test content"), 0o644); err != nil {
		t.Fatal(err)
	}

	// First run to create checksums
	opts := consumers.CheckCalcOptions{
		CalcCount:    1,
		Recalc:       false,
		Validate:     false,
		Scrub:        false,
		ShowProgress: false,
		AutoFix:      nil,
		Tuner:        nil,
	}

	if err := consumers.RunCheckCalc([]string{tmpDir}, opts); err != nil {
		t.Fatalf("Initial RunCheckCalc failed: %v", err)
	}

	// Second run with validation
	opts.Validate = true
	if err := consumers.RunCheckCalc([]string{tmpDir}, opts); err != nil {
		t.Fatalf("Validation RunCheckCalc failed: %v", err)
	}
}

// TestCheckCalcWithScrub tests scrub flag
func TestCheckCalcWithScrub(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test file
	testFile := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("test content"), 0o644); err != nil {
		t.Fatal(err)
	}

	// First run
	opts := consumers.CheckCalcOptions{
		CalcCount:    1,
		Recalc:       false,
		Validate:     false,
		Scrub:        false,
		ShowProgress: false,
		AutoFix:      nil,
		Tuner:        nil,
	}

	if err := consumers.RunCheckCalc([]string{tmpDir}, opts); err != nil {
		t.Fatalf("Initial RunCheckCalc failed: %v", err)
	}

	// Run with scrub flag
	opts.Scrub = true
	if err := consumers.RunCheckCalc([]string{tmpDir}, opts); err != nil {
		t.Fatalf("Scrub RunCheckCalc failed: %v", err)
	}
}

// TestCheckCalcWithMultipleCalcCount tests parallel calculation
func TestCheckCalcWithMultipleCalcCount(t *testing.T) {
	tmpDir := t.TempDir()

	// Create multiple test files
	for i := 0; i < 5; i++ {
		name := filepath.Join(tmpDir, "file0.txt")
		if i > 0 {
			name = filepath.Join(tmpDir, "file"+string(rune(48+i))+".txt")
		}
		if err := os.WriteFile(name, []byte("content"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Run with higher parallelism
	opts := consumers.CheckCalcOptions{
		CalcCount:    4,
		Recalc:       false,
		Validate:     false,
		Scrub:        false,
		ShowProgress: false,
		AutoFix:      nil,
		Tuner:        nil,
	}

	if err := consumers.RunCheckCalc([]string{tmpDir}, opts); err != nil {
		t.Fatalf("RunCheckCalc with CalcCount=4 failed: %v", err)
	}
}

// TestCheckCalcWithAdaptiveTuner tests adaptive tuning
func TestCheckCalcWithAdaptiveTuner(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test files
	for i := 0; i < 3; i++ {
		name := filepath.Join(tmpDir, "file"+string(rune(48+i))+".txt")
		if err := os.WriteFile(name, []byte("test content"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Create adaptive tuner
	tuner := adaptive.NewTunerWithConfig(1, 4, 0) // 0 interval to skip timing checks

	opts := consumers.CheckCalcOptions{
		CalcCount:    2,
		Recalc:       false,
		Validate:     false,
		Scrub:        false,
		ShowProgress: false,
		AutoFix:      nil,
		Tuner:        tuner,
	}

	if err := consumers.RunCheckCalc([]string{tmpDir}, opts); err != nil {
		t.Fatalf("RunCheckCalc with tuner failed: %v", err)
	}
}

// TestCheckCalcMultipleDirectories tests processing multiple directories
func TestCheckCalcMultipleDirectories(t *testing.T) {
	tmpDir1 := t.TempDir()
	tmpDir2 := t.TempDir()

	// Create files in both directories
	if err := os.WriteFile(filepath.Join(tmpDir1, "file1.txt"), []byte("content1"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir2, "file2.txt"), []byte("content2"), 0o644); err != nil {
		t.Fatal(err)
	}

	opts := consumers.CheckCalcOptions{
		CalcCount:    1,
		Recalc:       false,
		Validate:     false,
		Scrub:        false,
		ShowProgress: false,
		AutoFix:      nil,
		Tuner:        nil,
	}

	if err := consumers.RunCheckCalc([]string{tmpDir1, tmpDir2}, opts); err != nil {
		t.Fatalf("RunCheckCalc with multiple directories failed: %v", err)
	}

	// Verify both directories have .medorg.xml
	for _, dir := range []string{tmpDir1, tmpDir2} {
		mdFile := filepath.Join(dir, core.Md5FileName)
		if _, err := os.Stat(mdFile); os.IsNotExist(err) {
			t.Errorf("Expected %s in %s", core.Md5FileName, dir)
		}
	}
}

// TestCheckCalcEmptyDirectory tests handling of empty directories
func TestCheckCalcEmptyDirectory(t *testing.T) {
	tmpDir := t.TempDir()

	opts := consumers.CheckCalcOptions{
		CalcCount:    1,
		Recalc:       false,
		Validate:     false,
		Scrub:        false,
		ShowProgress: false,
		AutoFix:      nil,
		Tuner:        nil,
	}

	// Should not error on empty directory
	if err := consumers.RunCheckCalc([]string{tmpDir}, opts); err != nil {
		t.Fatalf("RunCheckCalc on empty directory failed: %v", err)
	}
}

// TestCheckCalcWithProgressCallback tests progress reporting
func TestCheckCalcWithProgressCallback(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test file
	if err := os.WriteFile(filepath.Join(tmpDir, "file.txt"), []byte("test content"), 0o644); err != nil {
		t.Fatal(err)
	}

	opts := consumers.CheckCalcOptions{
		CalcCount:    1,
		Recalc:       false,
		Validate:     false,
		Scrub:        false,
		ShowProgress: true, // Enable progress reporting
		AutoFix:      nil,
		Tuner:        nil,
	}

	if err := consumers.RunCheckCalc([]string{tmpDir}, opts); err != nil {
		t.Fatalf("RunCheckCalc with progress failed: %v", err)
	}
}

// TestCheckCalcSubdirectories tests recursive directory processing
func TestCheckCalcSubdirectories(t *testing.T) {
	tmpDir := t.TempDir()

	// Create nested directory structure
	subDir := filepath.Join(tmpDir, "subdir")
	if err := os.MkdirAll(subDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create files at different levels
	if err := os.WriteFile(filepath.Join(tmpDir, "root.txt"), []byte("root"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subDir, "sub.txt"), []byte("sub"), 0o644); err != nil {
		t.Fatal(err)
	}

	opts := consumers.CheckCalcOptions{
		CalcCount:    1,
		Recalc:       false,
		Validate:     false,
		Scrub:        false,
		ShowProgress: false,
		AutoFix:      nil,
		Tuner:        nil,
	}

	if err := consumers.RunCheckCalc([]string{tmpDir}, opts); err != nil {
		t.Fatalf("RunCheckCalc with subdirectories failed: %v", err)
	}

	// Verify .medorg.xml exists in both root and subdir
	for _, dir := range []string{tmpDir, subDir} {
		mdFile := filepath.Join(dir, core.Md5FileName)
		if _, err := os.Stat(mdFile); os.IsNotExist(err) {
			t.Errorf("Expected %s in %s", core.Md5FileName, dir)
		}
	}
}

// TestCheckCalcLargeFile tests handling of larger files
func TestCheckCalcLargeFile(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a 10MB file
	testFile := filepath.Join(tmpDir, "large.bin")
	f, err := os.Create(testFile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Write 10MB of data
	chunk := make([]byte, 1024*1024) // 1MB chunks
	for i := 0; i < 10; i++ {
		if _, err := f.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}

	opts := consumers.CheckCalcOptions{
		CalcCount:    1,
		Recalc:       false,
		Validate:     false,
		Scrub:        false,
		ShowProgress: false,
		AutoFix:      nil,
		Tuner:        nil,
	}

	if err := consumers.RunCheckCalc([]string{tmpDir}, opts); err != nil {
		t.Fatalf("RunCheckCalc on large file failed: %v", err)
	}
}
