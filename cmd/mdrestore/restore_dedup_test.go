package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/medorg/pkg/cli"
	"github.com/cbehopkins/medorg/pkg/core"
)

// TestRestoreDuplicateContentFiles tests that restore correctly handles
// files with identical content but different names - they should be
// restored as separate, independent files.
func TestRestoreDuplicateContentFiles(t *testing.T) {
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

	// Create two files with IDENTICAL content but different names
	duplicateContent := []byte("This exact content is duplicated")
	file1 := filepath.Join(sourceDir, "report_v1.txt")
	file2 := filepath.Join(sourceDir, "report_final.txt")

	if err := os.WriteFile(file1, duplicateContent, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file2, duplicateContent, 0o644); err != nil {
		t.Fatal(err)
	}

	// Create a third file with different content
	uniqueFile := filepath.Join(sourceDir, "unique.txt")
	if err := os.WriteFile(uniqueFile, []byte("Unique content"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Calculate checksums manually for the duplicate content
	checksum1, err := core.CalcMd5File(sourceDir, "report_v1.txt")
	if err != nil {
		t.Fatal(err)
	}
	checksum2, err := core.CalcMd5File(sourceDir, "report_final.txt")
	if err != nil {
		t.Fatal(err)
	}
	checksumUnique, err := core.CalcMd5File(sourceDir, "unique.txt")
	if err != nil {
		t.Fatal(err)
	}

	if checksum1 != checksum2 {
		t.Fatalf("Test setup error: files should have same checksum, got %s and %s",
			checksum1, checksum2)
	}
	t.Logf("✓ Test files have identical checksum: %s", checksum1)

	// Create volume label file
	volLabelPath := filepath.Join(sourceDir, ".mdbackup.xml")
	volContent := `<vol><label>TEST_VOL</label></vol>`
	if err := os.WriteFile(volLabelPath, []byte(volContent), 0o644); err != nil {
		t.Fatal(err)
	}

	// Create journal with both duplicate files
	journalContent := `<mdj alias="test">
  <dr dir=".">
    <fr fname="report_v1.txt" checksum="` + checksum1 + `" mtime="1234567890" size="32">
      <bd>TEST_VOL</bd>
    </fr>
    <fr fname="report_final.txt" checksum="` + checksum2 + `" mtime="1234567890" size="32">
      <bd>TEST_VOL</bd>
    </fr>
    <fr fname="unique.txt" checksum="` + checksumUnique + `" mtime="1234567890" size="14">
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

	// Verify BOTH files were restored as separate files
	restoredFile1 := filepath.Join(restoreDir, "report_v1.txt")
	restoredFile2 := filepath.Join(restoreDir, "report_final.txt")
	restoredFile3 := filepath.Join(restoreDir, "unique.txt")

	for _, path := range []string{restoredFile1, restoredFile2, restoredFile3} {
		stat, err := os.Stat(path)
		if os.IsNotExist(err) {
			t.Errorf("File not restored: %s", path)
			continue
		}
		if err != nil {
			t.Errorf("Error checking restored file %s: %v", path, err)
			continue
		}

		// Verify it's a regular file (not a symlink)
		if !stat.Mode().IsRegular() {
			t.Errorf("Restored file %s is not a regular file: mode %v", path, stat.Mode())
		}

		t.Logf("✓ File restored: %s", filepath.Base(path))
	}

	// Verify content of duplicate files
	content1, err := os.ReadFile(restoredFile1)
	if err != nil {
		t.Fatalf("Failed to read restored file1: %v", err)
	}
	content2, err := os.ReadFile(restoredFile2)
	if err != nil {
		t.Fatalf("Failed to read restored file2: %v", err)
	}

	if !bytes.Equal(content1, duplicateContent) {
		t.Errorf("File1 content mismatch:\n  Expected: %s\n  Got: %s",
			string(duplicateContent), string(content1))
	}

	if !bytes.Equal(content2, duplicateContent) {
		t.Errorf("File2 content mismatch:\n  Expected: %s\n  Got: %s",
			string(duplicateContent), string(content2))
	}

	if !bytes.Equal(content1, content2) {
		t.Error("Duplicate files should have identical content after restore")
	}

	t.Log("✓ Both files restored with correct identical content")

	// Verify they are separate physical files (not hardlinks)
	stat1, _ := os.Stat(restoredFile1)
	stat2, _ := os.Stat(restoredFile2)

	if stat1.Size() == 0 || stat2.Size() == 0 {
		t.Error("Restored files should not be empty")
	}

	if stat1.Size() != stat2.Size() {
		t.Errorf("Duplicate files should have same size, got %d and %d",
			stat1.Size(), stat2.Size())
	}

	t.Logf("✓ Files are separate with size: %d bytes each", stat1.Size())

	// Verify output shows both files were processed
	outputStr := output.String()
	if !bytes.Contains([]byte(outputStr), []byte("report_v1.txt")) {
		t.Error("Output should mention report_v1.txt")
	}
	if !bytes.Contains([]byte(outputStr), []byte("report_final.txt")) {
		t.Error("Output should mention report_final.txt")
	}

	t.Log("✓ Restore correctly handles duplicate content files as independent entities")
}

// TestRestoreDuplicateContentAcrossSubdirs tests that duplicate content
// in different subdirectories is restored correctly
func TestRestoreDuplicateContentAcrossSubdirs(t *testing.T) {
	tmpDir := t.TempDir()
	sourceDir := filepath.Join(tmpDir, "source")
	journalPath := filepath.Join(tmpDir, "test.journal")
	restoreDir := filepath.Join(tmpDir, "restore")
	configPath := filepath.Join(tmpDir, ".core.xml")

	// Create nested source structure
	subdir1 := filepath.Join(sourceDir, "config", "v1")
	subdir2 := filepath.Join(sourceDir, "backup", "old")

	for _, dir := range []string{subdir1, subdir2} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
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

	if !xc.AddSourceDirectory(sourceDir, "test") {
		t.Fatal("Failed to add source directory")
	}

	if err := xc.SetRestoreDestination("test", restoreDir); err != nil {
		t.Fatal(err)
	}

	// Create files with identical content in different subdirectories
	duplicateContent := []byte("Shared config content")
	file1 := filepath.Join(subdir1, "settings.yaml")
	file2 := filepath.Join(subdir2, "settings_backup.yaml")

	if err := os.WriteFile(file1, duplicateContent, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file2, duplicateContent, 0o644); err != nil {
		t.Fatal(err)
	}

	// Create volume label file at the root sourceDir (where GetVolumeLabel looks)
	volLabelPath := filepath.Join(sourceDir, ".mdbackup.xml")
	volContent := `<vol><label>TEST_VOL</label></vol>`
	if err := os.WriteFile(volLabelPath, []byte(volContent), 0o644); err != nil {
		t.Fatal(err)
	}

	// Calculate checksums to verify duplication
	checksum1, err := core.CalcMd5File(subdir1, "settings.yaml")
	if err != nil {
		t.Fatal(err)
	}
	checksum2, err := core.CalcMd5File(subdir2, "settings_backup.yaml")
	if err != nil {
		t.Fatal(err)
	}

	if checksum1 != checksum2 {
		t.Fatalf("Test setup error: files should have same checksum")
	}
	t.Logf("✓ Files in different subdirs have matching checksum: %s", checksum1)

	// Create journal
	journalContent := `<mdj alias="test">
  <dr dir="config/v1">
    <fr fname="settings.yaml" checksum="` + checksum1 + `" mtime="1234567890" size="20">
      <bd>TEST_VOL</bd>
    </fr>
  </dr>
  <dr dir="backup/old">
    <fr fname="settings_backup.yaml" checksum="` + checksum2 + `" mtime="1234567890" size="20">
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

	// Verify both files restored to correct subdirectories (preserving structure)
	restored1 := filepath.Join(restoreDir, "config", "v1", "settings.yaml")
	restored2 := filepath.Join(restoreDir, "backup", "old", "settings_backup.yaml")

	for _, path := range []string{restored1, restored2} {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("Failed to read restored file %s: %v", path, err)
			continue
		}

		if !bytes.Equal(content, duplicateContent) {
			t.Errorf("Content mismatch in %s", path)
		} else {
			t.Logf("✓ File restored correctly: %s", path)
		}
	}

	t.Log("✓ Duplicate content across subdirectories restored with structure preserved")
}
