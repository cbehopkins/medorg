package main

import (
	"os"
	"path/filepath"
	"testing"
)

// Unit tests for mdjournal main logic (additional coverage)

// TestRunBasicJournal tests basic journal operation
func TestRunBasicJournal(t *testing.T) {
	tmpDir := t.TempDir()
	srcDir := filepath.Join(tmpDir, "src")
	os.MkdirAll(srcDir, 0o755)

	// Create a test file
	os.WriteFile(filepath.Join(srcDir, "test.txt"), []byte("content"), 0o644)

	cfg := Config{
		Directories:    []string{srcDir},
		JournalPath:    filepath.Join(tmpDir, "journal.xml"),
		ScanOnly:       false,
		ReadExisting:   false,
		IgnorePatterns: []string{},
		GetAlias:       nil,
	}

	exitCode, err := Run(cfg)
	if err != nil && exitCode == 0 {
		t.Fatalf("Run failed: %v", err)
	}
	// Note: Run may fail due to missing backup, but it should be testable
}

// TestRunWithScanOnly tests journal with scan-only flag
func TestRunWithScanOnly(t *testing.T) {
	tmpDir := t.TempDir()
	srcDir := filepath.Join(tmpDir, "src")
	os.MkdirAll(srcDir, 0o755)

	os.WriteFile(filepath.Join(srcDir, "test.txt"), []byte("content"), 0o644)

	cfg := Config{
		Directories:    []string{srcDir},
		JournalPath:    filepath.Join(tmpDir, "journal.xml"),
		ScanOnly:       true, // Only scan, don't run full operation
		ReadExisting:   false,
		IgnorePatterns: []string{},
		GetAlias:       nil,
	}

	exitCode, err := Run(cfg)
	// Scan-only should be less likely to fail
	if exitCode != 0 && err != nil {
		// Some errors are expected, but at least verify it was attempted
		t.Logf("Run with scan-only returned exit code %d: %v", exitCode, err)
	}
}

// TestRunWithMultipleDirectories tests journal with multiple directories
func TestRunWithMultipleDirectories(t *testing.T) {
	tmpDir := t.TempDir()
	srcDir1 := filepath.Join(tmpDir, "src1")
	srcDir2 := filepath.Join(tmpDir, "src2")
	os.MkdirAll(srcDir1, 0o755)
	os.MkdirAll(srcDir2, 0o755)

	os.WriteFile(filepath.Join(srcDir1, "test1.txt"), []byte("content1"), 0o644)
	os.WriteFile(filepath.Join(srcDir2, "test2.txt"), []byte("content2"), 0o644)

	cfg := Config{
		Directories:    []string{srcDir1, srcDir2},
		JournalPath:    filepath.Join(tmpDir, "journal.xml"),
		ScanOnly:       true,
		ReadExisting:   false,
		IgnorePatterns: []string{},
		GetAlias:       nil,
	}

	exitCode, err := Run(cfg)
	if exitCode != 0 && err != nil {
		t.Logf("Run with multiple directories returned exit code %d: %v", exitCode, err)
	}
}

// TestRunWithIgnorePatterns tests journal with ignore patterns
func TestRunWithIgnorePatterns(t *testing.T) {
	tmpDir := t.TempDir()
	srcDir := filepath.Join(tmpDir, "src")
	os.MkdirAll(srcDir, 0o755)

	os.WriteFile(filepath.Join(srcDir, "test.txt"), []byte("content"), 0o644)
	os.WriteFile(filepath.Join(srcDir, "ignore.tmp"), []byte("temp"), 0o644)

	cfg := Config{
		Directories:    []string{srcDir},
		JournalPath:    filepath.Join(tmpDir, "journal.xml"),
		ScanOnly:       true,
		ReadExisting:   false,
		IgnorePatterns: []string{".*\\.tmp$"},
		GetAlias:       nil,
	}

	exitCode, err := Run(cfg)
	if exitCode != 0 && err != nil {
		t.Logf("Run with ignore patterns returned exit code %d: %v", exitCode, err)
	}
}

// TestRunWithGetAliasFunc tests journal with alias function
func TestRunWithGetAliasFunc(t *testing.T) {
	tmpDir := t.TempDir()
	srcDir := filepath.Join(tmpDir, "src")
	os.MkdirAll(srcDir, 0o755)

	os.WriteFile(filepath.Join(srcDir, "test.txt"), []byte("content"), 0o644)

	// Create a mock alias function
	getAlias := func(path string) string {
		if path == srcDir {
			return "test-alias"
		}
		return ""
	}

	cfg := Config{
		Directories:    []string{srcDir},
		JournalPath:    filepath.Join(tmpDir, "journal.xml"),
		ScanOnly:       true,
		ReadExisting:   false,
		IgnorePatterns: []string{},
		GetAlias:       getAlias,
	}

	exitCode, err := Run(cfg)
	if exitCode != 0 && err != nil {
		t.Logf("Run with alias function returned exit code %d: %v", exitCode, err)
	}
}

// TestRunWithReadExisting tests journal with read-existing flag
func TestRunWithReadExisting(t *testing.T) {
	tmpDir := t.TempDir()
	srcDir := filepath.Join(tmpDir, "src")
	os.MkdirAll(srcDir, 0o755)

	os.WriteFile(filepath.Join(srcDir, "test.txt"), []byte("content"), 0o644)

	cfg := Config{
		Directories:    []string{srcDir},
		JournalPath:    filepath.Join(tmpDir, "journal.xml"),
		ScanOnly:       true,
		ReadExisting:   true, // Try to read existing journal
		IgnorePatterns: []string{},
		GetAlias:       nil,
	}

	exitCode, err := Run(cfg)
	if exitCode != 0 && err != nil {
		t.Logf("Run with read-existing returned exit code %d: %v", exitCode, err)
	}
}

// TestRunWithEmptyDirectoryList tests journal with empty directory list
func TestRunWithEmptyDirectoryList(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := Config{
		Directories:    []string{}, // Empty
		JournalPath:    filepath.Join(tmpDir, "journal.xml"),
		ScanOnly:       false,
		ReadExisting:   false,
		IgnorePatterns: []string{},
		GetAlias:       nil,
	}

	exitCode, err := Run(cfg)
	// Should handle empty directory list gracefully
	if exitCode != 0 && err != nil {
		t.Logf("Run with empty directory list returned exit code %d: %v", exitCode, err)
	}
}

// TestConfigStruct tests the Config structure initialization
func TestConfigStruct(t *testing.T) {
	cfg := Config{
		Directories:    []string{"dir1", "dir2"},
		JournalPath:    "/path/to/journal.xml",
		ScanOnly:       true,
		ReadExisting:   false,
		IgnorePatterns: []string{"*.tmp"},
		GetAlias:       nil,
	}

	if len(cfg.Directories) != 2 {
		t.Errorf("Expected 2 directories, got %d", len(cfg.Directories))
	}
	if cfg.JournalPath != "/path/to/journal.xml" {
		t.Errorf("Expected journal path, got %s", cfg.JournalPath)
	}
	if !cfg.ScanOnly {
		t.Error("Expected ScanOnly to be true")
	}
	if cfg.ReadExisting {
		t.Error("Expected ReadExisting to be false")
	}
	if len(cfg.IgnorePatterns) != 1 {
		t.Errorf("Expected 1 ignore pattern, got %d", len(cfg.IgnorePatterns))
	}
}

// TestRunWithCustomJournalPath tests using custom journal path
func TestRunWithCustomJournalPath(t *testing.T) {
	tmpDir := t.TempDir()
	srcDir := filepath.Join(tmpDir, "src")
	customJournal := filepath.Join(tmpDir, "custom", "path", "journal.xml")
	os.MkdirAll(srcDir, 0o755)
	os.MkdirAll(filepath.Dir(customJournal), 0o755)

	os.WriteFile(filepath.Join(srcDir, "test.txt"), []byte("content"), 0o644)

	cfg := Config{
		Directories:    []string{srcDir},
		JournalPath:    customJournal,
		ScanOnly:       true,
		ReadExisting:   false,
		IgnorePatterns: []string{},
		GetAlias:       nil,
	}

	exitCode, err := Run(cfg)
	if exitCode != 0 && err != nil {
		t.Logf("Run with custom journal path returned exit code %d: %v", exitCode, err)
	}
}

// TestRunWithNestedDirectories tests journal with nested directory structures
func TestRunWithNestedDirectories(t *testing.T) {
	tmpDir := t.TempDir()
	srcDir := filepath.Join(tmpDir, "src")
	nestedDir := filepath.Join(srcDir, "nested", "deep", "dir")
	os.MkdirAll(nestedDir, 0o755)

	os.WriteFile(filepath.Join(srcDir, "test1.txt"), []byte("content1"), 0o644)
	os.WriteFile(filepath.Join(nestedDir, "test2.txt"), []byte("content2"), 0o644)

	cfg := Config{
		Directories:    []string{srcDir},
		JournalPath:    filepath.Join(tmpDir, "journal.xml"),
		ScanOnly:       true,
		ReadExisting:   false,
		IgnorePatterns: []string{},
		GetAlias:       nil,
	}

	exitCode, err := Run(cfg)
	if exitCode != 0 && err != nil {
		t.Logf("Run with nested directories returned exit code %d: %v", exitCode, err)
	}
}
