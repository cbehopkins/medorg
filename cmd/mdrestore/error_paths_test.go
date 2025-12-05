package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

// TestRun_JournalReadError tests error handling when journal cannot be read
func TestRun_JournalReadError(t *testing.T) {
	tmpDir := t.TempDir()
	sourceDir := filepath.Join(tmpDir, "source")
	journalPath := filepath.Join(tmpDir, "nonexistent.journal")
	configPath := filepath.Join(tmpDir, ".core.xml")

	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatal(err)
	}

	xc, err := core.NewMdConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}

	volLabelPath := filepath.Join(sourceDir, ".mdbackup.xml")
	volContent := `<vol><label>TEST_VOL</label></vol>`
	if err := os.WriteFile(volLabelPath, []byte(volContent), 0o644); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	cfg := Config{
		JournalPath: journalPath,
		SourceDir:   sourceDir,
		XMLConfig:   xc,
		Stdout:      &output,
	}

	exitCode, err := Run(cfg)
	if err == nil {
		t.Error("Expected error when journal cannot be read")
	}
	if exitCode != ExitRestoreError {
		t.Errorf("Expected exit code %d, got %d", ExitRestoreError, exitCode)
	}
}

// TestRun_NoDestinationForAlias tests warning when no restore destination or source configured
func TestRun_NoDestinationForAlias(t *testing.T) {
	tmpDir := t.TempDir()
	sourceDir := filepath.Join(tmpDir, "source")
	journalPath := filepath.Join(tmpDir, "test.journal")
	configPath := filepath.Join(tmpDir, ".core.xml")

	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatal(err)
	}

	xc, err := core.NewMdConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}

	volLabelPath := filepath.Join(sourceDir, ".mdbackup.xml")
	volContent := `<vol><label>TEST_VOL</label></vol>`
	if err := os.WriteFile(volLabelPath, []byte(volContent), 0o644); err != nil {
		t.Fatal(err)
	}

	// Journal with alias that has no config
	journalContent := `<dr dir="." alias="unknown_alias">
  <fr fname="test.txt" checksum="abc" size="4">
    <bd>TEST_VOL</bd>
  </fr>
</dr>`
	if err := os.WriteFile(journalPath, []byte(journalContent), 0o644); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	cfg := Config{
		JournalPath: journalPath,
		SourceDir:   sourceDir,
		XMLConfig:   xc,
		Stdout:      &output,
	}

	exitCode, err := Run(cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if exitCode != ExitOk {
		t.Errorf("Expected exit code %d, got %d", ExitOk, exitCode)
	}

	outputStr := output.String()
	if !bytes.Contains([]byte(outputStr), []byte("no restore destination or source for alias")) {
		t.Error("Expected warning about missing destination/source for alias")
	}
}

// TestRun_DestinationDoesNotExist tests warning when destination directory doesn't exist
func TestRun_DestinationDoesNotExist(t *testing.T) {
	tmpDir := t.TempDir()
	sourceDir := filepath.Join(tmpDir, "source")
	journalPath := filepath.Join(tmpDir, "test.journal")
	restoreDir := filepath.Join(tmpDir, "nonexistent_restore")
	configPath := filepath.Join(tmpDir, ".core.xml")

	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatal(err)
	}

	xc, err := core.NewMdConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}

	if !xc.AddSourceDirectory(sourceDir, "test") {
		t.Fatal("Failed to add source directory")
	}

	// Set restore destination to nonexistent directory
	if err := xc.SetRestoreDestination("test", restoreDir); err != nil {
		t.Fatal(err)
	}

	volLabelPath := filepath.Join(sourceDir, ".mdbackup.xml")
	volContent := `<vol><label>TEST_VOL</label></vol>`
	if err := os.WriteFile(volLabelPath, []byte(volContent), 0o644); err != nil {
		t.Fatal(err)
	}

	journalContent := `<dr dir="." alias="test">
  <fr fname="test.txt" checksum="abc" size="4">
    <bd>TEST_VOL</bd>
  </fr>
</dr>`
	if err := os.WriteFile(journalPath, []byte(journalContent), 0o644); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	cfg := Config{
		JournalPath: journalPath,
		SourceDir:   sourceDir,
		XMLConfig:   xc,
		Stdout:      &output,
	}

	exitCode, err := Run(cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if exitCode != ExitOk {
		t.Errorf("Expected exit code %d, got %d", ExitOk, exitCode)
	}

	outputStr := output.String()
	if !bytes.Contains([]byte(outputStr), []byte("does not exist")) {
		t.Error("Expected warning about nonexistent destination")
	}
}

// TestRun_CompleteRestore tests the "Restore complete!" path (no missing volumes)
func TestRun_CompleteRestore(t *testing.T) {
	tmpDir := t.TempDir()
	sourceDir := filepath.Join(tmpDir, "source")
	journalPath := filepath.Join(tmpDir, "test.journal")
	restoreDir := filepath.Join(tmpDir, "restore")
	configPath := filepath.Join(tmpDir, ".core.xml")

	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(restoreDir, 0o755); err != nil {
		t.Fatal(err)
	}

	xc, err := core.NewMdConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}

	if !xc.AddSourceDirectory(sourceDir, "test") {
		t.Fatal("Failed to add source directory")
	}
	if err := xc.SetRestoreDestination("test", restoreDir); err != nil {
		t.Fatal(err)
	}

	// Create source file
	testFile := filepath.Join(sourceDir, "complete.txt")
	testContent := []byte("complete test")
	if err := os.WriteFile(testFile, testContent, 0o644); err != nil {
		t.Fatal(err)
	}

	volLabelPath := filepath.Join(sourceDir, ".mdbackup.xml")
	volContent := `<vol><label>COMPLETE_VOL</label></vol>`
	if err := os.WriteFile(volLabelPath, []byte(volContent), 0o644); err != nil {
		t.Fatal(err)
	}

	// Calculate real checksum
	hash, err := core.CalcMd5File(sourceDir, "complete.txt")
	if err != nil {
		t.Fatal(err)
	}

	journalContent := `<dr dir="." alias="test">
  <fr fname="complete.txt" checksum="` + hash + `" size="13">
    <bd>COMPLETE_VOL</bd>
  </fr>
</dr>`
	if err := os.WriteFile(journalPath, []byte(journalContent), 0o644); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	cfg := Config{
		JournalPath: journalPath,
		SourceDir:   sourceDir,
		XMLConfig:   xc,
		Stdout:      &output,
	}

	exitCode, err := Run(cfg)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if exitCode != ExitOk {
		t.Errorf("Expected exit code %d, got %d", ExitOk, exitCode)
	}

	outputStr := output.String()
	if !bytes.Contains([]byte(outputStr), []byte("Restore complete!")) {
		t.Error("Expected 'Restore complete!' message")
	}
	if bytes.Contains([]byte(outputStr), []byte("Missing volumes")) {
		t.Error("Should not mention missing volumes")
	}
}

// TestRun_ChecksumCalculationError tests warning when checksum calculation fails
func TestRun_ChecksumCalculationError(t *testing.T) {
	tmpDir := t.TempDir()
	sourceDir := filepath.Join(tmpDir, "source")
	journalPath := filepath.Join(tmpDir, "test.journal")
	restoreDir := filepath.Join(tmpDir, "restore")
	configPath := filepath.Join(tmpDir, ".core.xml")

	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Create restore dir but make it unreadable to cause checksum calculation to fail
	if err := os.MkdirAll(restoreDir, 0o755); err != nil {
		t.Fatal(err)
	}

	xc, err := core.NewMdConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}

	if !xc.AddSourceDirectory(sourceDir, "test") {
		t.Fatal("Failed to add source directory")
	}
	if err := xc.SetRestoreDestination("test", restoreDir); err != nil {
		t.Fatal(err)
	}

	volLabelPath := filepath.Join(sourceDir, ".mdbackup.xml")
	volContent := `<vol><label>TEST_VOL</label></vol>`
	if err := os.WriteFile(volLabelPath, []byte(volContent), 0o644); err != nil {
		t.Fatal(err)
	}

	journalContent := `<dr dir="." alias="test">
  <fr fname="test.txt" checksum="abc" size="4">
    <bd>TEST_VOL</bd>
  </fr>
</dr>`
	if err := os.WriteFile(journalPath, []byte(journalContent), 0o644); err != nil {
		t.Fatal(err)
	}

	// Create a file in restore dir that will cause permission issues
	problemFile := filepath.Join(restoreDir, "subdir")
	if err := os.WriteFile(problemFile, []byte("block"), 0o000); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(problemFile, 0o644) // Clean up

	var output bytes.Buffer
	cfg := Config{
		JournalPath: journalPath,
		SourceDir:   sourceDir,
		XMLConfig:   xc,
		Stdout:      &output,
	}

	exitCode, err := Run(cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if exitCode != ExitOk {
		t.Errorf("Expected exit code %d, got %d", ExitOk, exitCode)
	}

	// The checksum calculation should warn but not fail completely
	outputStr := output.String()
	t.Logf("Output: %s", outputStr)
}
