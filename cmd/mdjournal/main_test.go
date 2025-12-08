package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cbehopkins/medorg/pkg/cli"
	"github.com/cbehopkins/medorg/pkg/consumers"
	"github.com/cbehopkins/medorg/pkg/core"
)

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
	}

	exitCode, err := Run(cfg)
	if exitCode != cli.ExitOk || err != nil {
		t.Fatalf("Run() failed: exit=%d, err=%v", exitCode, err)
	}

	// Read journal and verify file is present
	fh, _ := os.Open(journalPath)
	defer fh.Close()
	journal := consumers.Journal{}
	journal.FromReader(fh)

	foundFile := false
	journal.Range(func(de core.DirectoryEntryJournalableInterface, dir string) error {
		de.Revisit(dir, func(dm core.DirectoryEntryInterface, directory string, file string, fs core.FileStruct) error {
			if fs.Name == "test.txt" {
				foundFile = true
			}
			return nil
		})
		return nil
	})

	if !foundFile {
		t.Error("test.txt not found in journal")
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
	}

	exitCode, err := Run(cfg)
	if exitCode != cli.ExitOk || err != nil {
		t.Fatalf("Run() failed: exit=%d, err=%v", exitCode, err)
	}

	// Verify both files are in journal
	fh, _ := os.Open(journalPath)
	defer fh.Close()
	journal := consumers.Journal{}
	journal.FromReader(fh)

	foundFiles := make(map[string]bool)
	journal.Range(func(de core.DirectoryEntryJournalableInterface, dir string) error {
		de.Revisit(dir, func(dm core.DirectoryEntryInterface, directory string, file string, fs core.FileStruct) error {
			foundFiles[fs.Name] = true
			return nil
		})
		return nil
	})

	if !foundFiles["file1.txt"] || !foundFiles["file2.txt"] {
		t.Error("Files from both directories should be in journal")
	}
}
