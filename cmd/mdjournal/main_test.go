package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cbehopkins/medorg/pkg/cli"
	"github.com/cbehopkins/medorg/pkg/consumers"
)

func aliasFromPath(path string) string {
	alias := filepath.Base(path)
	if alias == "" || alias == "." || alias == string(filepath.Separator) {
		return path
	}
	return alias
}

// Integration tests - these test the actual Run() function and CLI behavior

func TestIntegration_EmptyDirectory(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "mdjournal-integ-*")
	defer os.RemoveAll(tempDir)

	journalPath := filepath.Join(tempDir, ".mdjournal.xml")
	var stdout bytes.Buffer

	cfg := Config{
		Directories:  []string{tempDir},
		JournalPath:  journalPath,
		Stdout:       &stdout,
		ReadExisting: false,
		GetAlias:     aliasFromPath,
	}

	exitCode, err := Run(cfg)
	if exitCode != cli.ExitOk {
		t.Errorf("Expected exit code %d, got %d", cli.ExitOk, exitCode)
	}
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify journal file was created
	if _, err := os.Stat(journalPath); os.IsNotExist(err) {
		t.Error("Journal file was not created")
	}
}

func TestIntegration_SingleFile(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "mdjournal-integ-*")
	defer os.RemoveAll(tempDir)

	// Create test file
	os.WriteFile(filepath.Join(tempDir, "test.txt"), []byte("content"), 0o644)

	journalPath := filepath.Join(tempDir, ".mdjournal.xml")
	var stdout bytes.Buffer

	cfg := Config{
		Directories:  []string{tempDir},
		JournalPath:  journalPath,
		Stdout:       &stdout,
		ReadExisting: false,
		GetAlias:     aliasFromPath,
	}

	exitCode, err := Run(cfg)
	if exitCode != cli.ExitOk || err != nil {
		t.Fatalf("Run() failed: exit=%d, err=%v", exitCode, err)
	}

	// Read journal and verify it contains expected content
	fh, _ := os.Open(journalPath)
	defer fh.Close()
	journal, err := consumers.NewJournal()
	if err != nil {
		t.Fatal(err)
	}

	if err := journal.FromReader(fh); err != nil {
		t.Fatalf("Failed to read journal: %v", err)
	}

	// Verify journal has content
	journalStr := journal.String()
	if journalStr == "" {
		t.Error("Journal should not be empty after run")
	}

	// Verify journal contains alias in XML
	journalContent, _ := os.ReadFile(journalPath)
	if !strings.Contains(string(journalContent), "alias") {
		t.Error("Journal should contain alias attribute")
	}
}

func TestIntegration_ScanFlag(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "mdjournal-integ-*")
	defer os.RemoveAll(tempDir)

	journalPath := filepath.Join(tempDir, ".mdjournal.xml")
	var stdout bytes.Buffer

	cfg := Config{
		Directories:  []string{tempDir},
		JournalPath:  journalPath,
		Stdout:       &stdout,
		ScanOnly:     true,
		ReadExisting: false,
		GetAlias:     aliasFromPath,
	}

	Run(cfg)

	output := stdout.String()
	if !strings.Contains(output, "scan") {
		t.Errorf("Expected scan message in output, got: %s", output)
	}
}

func TestIntegration_ReadExistingJournal(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "mdjournal-integ-*")
	defer os.RemoveAll(tempDir)

	journalPath := filepath.Join(tempDir, ".mdjournal.xml")

	// Create initial journal
	os.WriteFile(filepath.Join(tempDir, "file1.txt"), []byte("content1"), 0o644)
	cfg1 := Config{
		Directories:  []string{tempDir},
		JournalPath:  journalPath,
		ReadExisting: false,
		GetAlias:     aliasFromPath,
	}
	Run(cfg1)

	// Now add a new file and re-run with ReadExisting
	os.WriteFile(filepath.Join(tempDir, "file2.txt"), []byte("content2"), 0o644)
	var stdout bytes.Buffer
	cfg2 := Config{
		Directories:  []string{tempDir},
		JournalPath:  journalPath,
		Stdout:       &stdout,
		ReadExisting: true,
		GetAlias:     aliasFromPath,
	}
	Run(cfg2)

	// Should see "Reading in journal" message
	if !strings.Contains(stdout.String(), "Reading in journal") {
		t.Error("Expected 'Reading in journal' message")
	}
}

func TestIntegration_MultipleDirectories(t *testing.T) {
	tempDir1, _ := os.MkdirTemp("", "mdjournal-integ-*")
	defer os.RemoveAll(tempDir1)
	tempDir2, _ := os.MkdirTemp("", "mdjournal-integ-*")
	defer os.RemoveAll(tempDir2)

	// Create files in different directories
	os.WriteFile(filepath.Join(tempDir1, "file1.txt"), []byte("content1"), 0o644)
	os.WriteFile(filepath.Join(tempDir2, "file2.txt"), []byte("content2"), 0o644)

	journalPath := filepath.Join(tempDir1, ".mdjournal.xml")
	cfg := Config{
		Directories:  []string{tempDir1, tempDir2},
		JournalPath:  journalPath,
		ReadExisting: false,
		GetAlias:     aliasFromPath,
	}

	exitCode, err := Run(cfg)
	if exitCode != cli.ExitOk || err != nil {
		t.Fatalf("Run() failed: exit=%d, err=%v", exitCode, err)
	}

	// Read journal and verify it has content
	fh, _ := os.Open(journalPath)
	defer fh.Close()
	journal, err := consumers.NewJournal()
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.FromReader(fh); err != nil {
		t.Fatalf("Failed to read journal: %v", err)
	}

	journalStr := journal.String()
	if journalStr == "" {
		t.Error("Journal should not be empty after processing multiple directories")
	}
}
