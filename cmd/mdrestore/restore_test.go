package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/medorg/pkg/cli"
	"github.com/cbehopkins/medorg/pkg/consumers"
	"github.com/cbehopkins/medorg/pkg/core"
)

func TestMdrestoreBasic(t *testing.T) {
	// Create temporary directories
	tmpDir := t.TempDir()
	sourceDir := filepath.Join(tmpDir, "source")
	journalPath := filepath.Join(tmpDir, "test.journal")
	restoreDir := filepath.Join(tmpDir, "restore")
	configPath := filepath.Join(tmpDir, ".core.xml")

	// Create source directory
	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create restore directory
	if err := os.MkdirAll(restoreDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create XMLCfg
	xc, err := core.NewMdConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}

	// Add source directory with alias
	if !xc.AddSourceDirectory(sourceDir, "test") {
		t.Fatal("Failed to add source directory")
	}

	// Set restore destination
	if err := xc.SetRestoreDestination("test", restoreDir); err != nil {
		t.Fatal(err)
	}

	// Create test file in source
	testFile := filepath.Join(sourceDir, "testfile.txt")
	testContent := []byte("test content")
	if err := os.WriteFile(testFile, testContent, 0o644); err != nil {
		t.Fatal(err)
	}

	// Create volume label file in source
	volLabelPath := filepath.Join(sourceDir, ".mdbackup.xml")
	volContent := `<vol><label>TEST_VOL</label></vol>`
	if err := os.WriteFile(volLabelPath, []byte(volContent), 0o644); err != nil {
		t.Fatal(err)
	}

	// Create a simple journal entry
	journalContent := `<mdj alias="test">
  <dr dir=".">
    <fr fname="testfile.txt" checksum="mock_checksum" mtime="1234567890" size="12">
      <bd>TEST_VOL</bd>
    </fr>
  </dr>
</mdj>`
	if err := os.WriteFile(journalPath, []byte(journalContent), 0o644); err != nil {
		t.Fatal(err)
	}

	// Run restore
	var output bytes.Buffer
	cfg := Config{
		JournalPath: journalPath,
		SourceDir:   sourceDir,
		XMLConfig:   xc,
		Stdout:      &output,
	}

	exitCode, err := Run(cfg)
	if err != nil {
		t.Fatalf("Run failed: %v\nOutput: %s", err, output.String())
	}

	if exitCode != cli.ExitOk {
		t.Errorf("Expected exit code %d, got %d\nOutput: %s", cli.ExitOk, exitCode, output.String())
	}

	// Verify file was restored
	restoredFile := filepath.Join(restoreDir, "testfile.txt")
	if _, err := os.Stat(restoredFile); os.IsNotExist(err) {
		t.Errorf("Restored file does not exist: %s", restoredFile)
	}

	// Verify output contains expected messages
	outputStr := output.String()
	expectedMessages := []string{
		"Source volume label: TEST_VOL",
		"Alias 'test' -> ",
		"Restore",
	}
	for _, msg := range expectedMessages {
		if !bytes.Contains([]byte(outputStr), []byte(msg)) {
			t.Errorf("Output does not contain expected message: %s\nOutput: %s", msg, outputStr)
		}
	}
}

func TestMdrestoreMissingVolume(t *testing.T) {
	// Create temporary directories
	tmpDir := t.TempDir()
	sourceDir := filepath.Join(tmpDir, "source")
	journalPath := filepath.Join(tmpDir, "test.journal")
	restoreDir := filepath.Join(tmpDir, "restore")
	configPath := filepath.Join(tmpDir, ".core.xml")

	// Create source directory
	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create restore directory
	if err := os.MkdirAll(restoreDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create XMLCfg
	xc, err := core.NewMdConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}

	// Add source directory with alias
	if !xc.AddSourceDirectory(sourceDir, "test") {
		t.Fatal("Failed to add source directory")
	}

	// Set restore destination
	if err := xc.SetRestoreDestination("test", restoreDir); err != nil {
		t.Fatal(err)
	}

	// Create volume label file in source (different from journal)
	volLabelPath := filepath.Join(sourceDir, ".mdbackup.xml")
	volContent := `<vol><label>VOL_1</label></vol>`
	if err := os.WriteFile(volLabelPath, []byte(volContent), 0o644); err != nil {
		t.Fatal(err)
	}

	// Create a journal entry with file on different volume
	journalContent := `<mdj alias="test">
  <dr dir=".">
    <fr fname="testfile.txt" checksum="mock_checksum" mtime="1234567890" size="12">
      <bd>VOL_2</bd>
    </fr>
  </dr>
</mdj>`
	if err := os.WriteFile(journalPath, []byte(journalContent), 0o644); err != nil {
		t.Fatal(err)
	}

	// Run restore
	var output bytes.Buffer
	cfg := Config{
		JournalPath: journalPath,
		SourceDir:   sourceDir,
		XMLConfig:   xc,
		Stdout:      &output,
	}

	exitCode, err := Run(cfg)
	if err != nil {
		t.Fatalf("Run failed: %v\nOutput: %s", err, output.String())
	}

	if exitCode != cli.ExitOk {
		t.Errorf("Expected exit code %d, got %d\nOutput: %s", cli.ExitOk, exitCode, output.String())
	}

	// Verify output mentions missing volume
	outputStr := output.String()
	if !bytes.Contains([]byte(outputStr), []byte("VOL_2")) {
		t.Errorf("Output should mention missing volume VOL_2\nOutput: %s", outputStr)
	}

	if !bytes.Contains([]byte(outputStr), []byte("Missing volumes needed")) {
		t.Errorf("Output should mention missing volumes\nOutput: %s", outputStr)
	}
}

func TestParseJournalAndInsert(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "restore.db")

	db, err := consumers.OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed: %v", err)
	}
	defer db.Close()

	xc, err := core.NewMdConfig(filepath.Join(tmpDir, ".core.xml"))
	if err != nil {
		t.Fatalf("NewMdConfig failed: %v", err)
	}
	if err := xc.SetRestoreDestination("test", filepath.Join(tmpDir, "restore")); err != nil {
		t.Fatalf("SetRestoreDestination failed: %v", err)
	}

	journalContent := `<mdj alias="test">
  <dr dir="summer">
    <fr fname="photo.jpg" checksum="checksum123" mtime="1234567890" size="12" bd="VOL_A" />
  </dr>
</mdj>`

	inserted, failed, err := parseJournalAndInsert(bytes.NewReader([]byte(journalContent)), db, xc)
	if err != nil {
		t.Fatalf("parseJournalAndInsert failed: %v", err)
	}
	if inserted != 1 || failed != 0 {
		t.Fatalf("expected 1 inserted and 0 failed, got %d inserted and %d failed", inserted, failed)
	}

	results, err := db.FindPendingByContent("checksum123", 12)
	if err != nil {
		t.Fatalf("FindPendingByContent failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 restore target, got %d", len(results))
	}

	expectedPath := filepath.Join(tmpDir, "restore", "summer", "photo.jpg")
	if results[0].TargetAbsPath != expectedPath {
		t.Fatalf("unexpected target path: want %s got %s", expectedPath, results[0].TargetAbsPath)
	}

	if results[0].TaskID != fmt.Sprintf("%s:%d:%s", "checksum123", 12, expectedPath) {
		t.Fatalf("unexpected task id: %s", results[0].TaskID)
	}

	counts, err := db.CountPendingByBackupDest()
	if err != nil {
		t.Fatalf("CountPendingByBackupDest failed: %v", err)
	}
	if counts["VOL_A"] != 1 {
		t.Fatalf("expected VOL_A count to be 1, got %d", counts["VOL_A"])
	}
}
