package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/medorg/pkg/consumers"
)

// TestJournalRoundTrip tests that a journal can be written and read back
func TestJournalRoundTrip(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "mdjournal-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create test directory structure with files
	subDir := filepath.Join(tempDir, "subdir")
	if err := os.Mkdir(subDir, 0o755); err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}

	if err := os.WriteFile(filepath.Join(tempDir, "file1.txt"), []byte("content1"), 0o644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(subDir, "file2.txt"), []byte("content2"), 0o644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	// Run CheckCalc to generate .medorg.xml files
	checkCalcOpts := consumers.CheckCalcOptions{
		CalcCount: 1,
		Recalc:    false,
		Validate:  false,
		Scrub:     false,
		AutoFix:   nil,
	}
	if err := consumers.RunCheckCalc([]string{tempDir}, checkCalcOpts); err != nil {
		t.Fatalf("Failed to run CheckCalc: %v", err)
	}

	// Create journal and populate from directory
	journal := consumers.NewJournal()
	if err := journal.PopulateFromDirectories(tempDir, "test-alias"); err != nil {
		t.Fatalf("Failed to populate journal: %v", err)
	}

	// Write to file and read back
	journalPath := filepath.Join(tempDir, ".mdjournal.xml")
	fh, err := os.Create(journalPath)
	if err != nil {
		t.Fatalf("Failed to create journal file: %v", err)
	}
	if err := journal.ToWriter(fh); err != nil {
		fh.Close()
		t.Fatalf("Failed to write journal: %v", err)
	}
	fh.Close()

	// Read back and verify
	fhRead, err := os.Open(journalPath)
	if err != nil {
		t.Fatalf("Failed to open journal file: %v", err)
	}
	defer fhRead.Close()

	journalRead := consumers.NewJournal()
	if err := journalRead.FromReader(fhRead); err != nil {
		t.Fatalf("Failed to read journal: %v", err)
	}

	// Verify the journal has the expected entries
	if len(journalRead.String()) == 0 {
		t.Error("Journal should not be empty after reading")
	}
}

// TestJournalFromDirRecursion tests that PopulateFromDirectories recurses into subdirectories
func TestJournalFromDirRecursion(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "mdjournal-recur-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create nested directory structure
	dirs := []string{
		filepath.Join(tempDir, "level1"),
		filepath.Join(tempDir, "level1", "level2"),
		filepath.Join(tempDir, "level1", "level2", "level3"),
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("Failed to create dir: %v", err)
		}
	}

	// Put a file in each directory
	for i, dir := range dirs {
		filename := filepath.Join(dir, "file.txt")
		content := []byte("level " + string(rune('1'+i)))
		if err := os.WriteFile(filename, content, 0o644); err != nil {
			t.Fatalf("Failed to write file: %v", err)
		}
	}

	// Run CheckCalc to generate .medorg.xml files
	checkCalcOpts := consumers.CheckCalcOptions{
		CalcCount: 1,
		Recalc:    false,
		Validate:  false,
		Scrub:     false,
		AutoFix:   nil,
	}
	if err := consumers.RunCheckCalc([]string{tempDir}, checkCalcOpts); err != nil {
		t.Fatalf("Failed to run CheckCalc: %v", err)
	}

	// Populate journal
	journal := consumers.NewJournal()
	if err := journal.PopulateFromDirectories(tempDir, "nested"); err != nil {
		t.Fatalf("Failed to populate journal: %v", err)
	}

	// Verify that multiple directories were added to the journal
	// (this is a simple check - the journal should have entries from all levels)
	journalStr := journal.String()
	if journalStr == "" {
		t.Error("Journal should have entries after PopulateFromDirectories")
	}
}

// TestJournalAliasRequired tests that alias is required for journal entries
func TestJournalAliasRequired(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "mdjournal-alias-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a test file
	if err := os.WriteFile(filepath.Join(tempDir, "test.txt"), []byte("content"), 0o644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	// Run CheckCalc to generate .medorg.xml
	checkCalcOpts := consumers.CheckCalcOptions{
		CalcCount: 1,
		Recalc:    false,
		Validate:  false,
		Scrub:     false,
		AutoFix:   nil,
	}
	if err := consumers.RunCheckCalc([]string{tempDir}, checkCalcOpts); err != nil {
		t.Fatalf("Failed to run CheckCalc: %v", err)
	}

	// Try to populate without alias - should fail
	journal := consumers.NewJournal()
	err = journal.PopulateFromDirectories(tempDir, "")
	if err != consumers.ErrAliasRequired {
		t.Errorf("Expected ErrAliasRequired, got %v", err)
	}

	// Try to populate with valid alias - should succeed
	err = journal.PopulateFromDirectories(tempDir, "valid-alias")
	if err != nil {
		t.Errorf("Failed to populate with valid alias: %v", err)
	}
}
