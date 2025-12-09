package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/medorg/pkg/cli"
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

	// Create a simple journal entry in new format
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
