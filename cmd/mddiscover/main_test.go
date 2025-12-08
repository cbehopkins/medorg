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

// Helper: Create a test file
func createTestFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("Failed to create test file %s: %v", path, err)
	}
	return path
}

// Helper: Calculate checksums for a directory
func calcChecksumsFor(t *testing.T, dir string) {
	t.Helper()
	if err := consumers.RunCheckCalc([]string{dir}, consumers.CheckCalcOptions{
		CalcCount: 2,
		Recalc:    false,
	}); err != nil {
		t.Fatalf("Failed to calculate checksums for %s: %v", dir, err)
	}
}

// Helper: Create volume label for a directory
func createVolumeLabel(t *testing.T, dir string) string {
	t.Helper()
	xc := &core.MdConfig{}
	vc, err := xc.VolumeCfgFromDir(dir)
	if err != nil {
		t.Fatalf("Failed to create volume label for %s: %v", dir, err)
	}
	return vc.Label
}

// Helper: Check if a file has a specific backup destination tag
func hasBackupDest(t *testing.T, dir, filename, volumeLabel string) bool {
	t.Helper()
	dm, err := core.DirectoryMapFromDir(dir)
	if err != nil {
		t.Fatalf("Failed to load directory map for %s: %v", dir, err)
	}
	fs, ok := dm.Get(filename)
	if !ok {
		return false
	}
	return fs.HasTag(volumeLabel)
}

func TestDiscoverBasic(t *testing.T) {
	// Create temporary directories
	tmpDir := t.TempDir()
	sourceDir := filepath.Join(tmpDir, "source")
	destDir := filepath.Join(tmpDir, "dest")

	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create identical files in both directories
	createTestFile(t, sourceDir, "file1.txt", "content1")
	createTestFile(t, sourceDir, "file2.txt", "content2")
	createTestFile(t, destDir, "file1.txt", "content1")
	createTestFile(t, destDir, "file2.txt", "content2")

	// Calculate checksums
	calcChecksumsFor(t, sourceDir)
	calcChecksumsFor(t, destDir)

	// Create volume label for destination
	volumeLabel := createVolumeLabel(t, destDir)

	// Create config and add source directory
	configPath := filepath.Join(tmpDir, "config.xml")
	xc, err := core.NewMdConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to create config: %v", err)
	}
	xc.AddSourceDirectory(sourceDir, "test-source")
	if err := xc.WriteXmlCfg(); err != nil {
		t.Fatal(err)
	}

	// Run discovery
	var out bytes.Buffer
	cfg := Config{
		SourceDirs:     []string{sourceDir},
		DestinationDir: destDir,
		ConfigPath:     configPath,
		Stdout:         &out,
		XMLConfig:      xc,
		DryRun:         false,
	}

	exitCode, err := Run(cfg)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if exitCode != cli.ExitOk {
		t.Fatalf("Expected exit code %d, got %d\nOutput: %s", cli.ExitOk, exitCode, out.String())
	}

	// Verify files are tagged
	if !hasBackupDest(t, sourceDir, "file1.txt", volumeLabel) {
		t.Error("file1.txt should be tagged with volume label")
	}
	if !hasBackupDest(t, sourceDir, "file2.txt", volumeLabel) {
		t.Error("file2.txt should be tagged with volume label")
	}

	// Verify output
	output := out.String()
	if !strings.Contains(output, "Files matched: 2") {
		t.Errorf("Expected 2 matches in output, got: %s", output)
	}
	if !strings.Contains(output, "Metadata updated: 2") {
		t.Errorf("Expected 2 updates in output, got: %s", output)
	}
}

func TestDiscoverDryRun(t *testing.T) {
	// Create temporary directories
	tmpDir := t.TempDir()
	sourceDir := filepath.Join(tmpDir, "source")
	destDir := filepath.Join(tmpDir, "dest")

	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create identical file
	createTestFile(t, sourceDir, "test.txt", "content")
	createTestFile(t, destDir, "test.txt", "content")

	calcChecksumsFor(t, sourceDir)
	calcChecksumsFor(t, destDir)

	volumeLabel := createVolumeLabel(t, destDir)

	// Create config
	configPath := filepath.Join(tmpDir, "config.xml")
	xc, _ := core.NewMdConfig(configPath)
	xc.AddSourceDirectory(sourceDir, "test-source")
	xc.WriteXmlCfg()

	// Run discovery in dry-run mode
	var out bytes.Buffer
	cfg := Config{
		SourceDirs:     []string{sourceDir},
		DestinationDir: destDir,
		ConfigPath:     configPath,
		Stdout:         &out,
		XMLConfig:      xc,
		DryRun:         true,
	}

	exitCode, err := Run(cfg)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if exitCode != cli.ExitOk {
		t.Fatalf("Expected exit code %d, got %d", cli.ExitOk, exitCode)
	}

	// Verify file is NOT tagged (dry run)
	if hasBackupDest(t, sourceDir, "test.txt", volumeLabel) {
		t.Error("file should NOT be tagged in dry-run mode")
	}

	// Verify output mentions dry run
	output := out.String()
	if !strings.Contains(output, "DRY RUN") {
		t.Errorf("Expected DRY RUN in output, got: %s", output)
	}
	if !strings.Contains(output, "Would update metadata for 1") {
		t.Errorf("Expected 'Would update' in output, got: %s", output)
	}
}

func TestDiscoverPartialMatch(t *testing.T) {
	// Create temporary directories
	tmpDir := t.TempDir()
	sourceDir := filepath.Join(tmpDir, "source")
	destDir := filepath.Join(tmpDir, "dest")

	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create files - only file1 exists on destination
	createTestFile(t, sourceDir, "file1.txt", "content1")
	createTestFile(t, sourceDir, "file2.txt", "content2")
	createTestFile(t, sourceDir, "file3.txt", "content3")
	createTestFile(t, destDir, "file1.txt", "content1")

	calcChecksumsFor(t, sourceDir)
	calcChecksumsFor(t, destDir)

	volumeLabel := createVolumeLabel(t, destDir)

	// Create config
	configPath := filepath.Join(tmpDir, "config.xml")
	xc, _ := core.NewMdConfig(configPath)
	xc.AddSourceDirectory(sourceDir, "test-source")
	xc.WriteXmlCfg()

	// Run discovery
	var out bytes.Buffer
	cfg := Config{
		SourceDirs:     []string{sourceDir},
		DestinationDir: destDir,
		ConfigPath:     configPath,
		Stdout:         &out,
		XMLConfig:      xc,
		DryRun:         false,
	}

	exitCode, _ := Run(cfg)
	if exitCode != cli.ExitOk {
		t.Fatalf("Expected exit code %d, got %d", cli.ExitOk, exitCode)
	}

	// Verify only file1 is tagged
	if !hasBackupDest(t, sourceDir, "file1.txt", volumeLabel) {
		t.Error("file1.txt should be tagged")
	}
	if hasBackupDest(t, sourceDir, "file2.txt", volumeLabel) {
		t.Error("file2.txt should NOT be tagged")
	}
	if hasBackupDest(t, sourceDir, "file3.txt", volumeLabel) {
		t.Error("file3.txt should NOT be tagged")
	}

	// Verify counts
	output := out.String()
	if !strings.Contains(output, "Files matched: 1") {
		t.Errorf("Expected 1 match, got: %s", output)
	}
}

func TestDiscoverAlreadyTagged(t *testing.T) {
	// Create temporary directories
	tmpDir := t.TempDir()
	sourceDir := filepath.Join(tmpDir, "source")
	destDir := filepath.Join(tmpDir, "dest")

	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create identical file
	createTestFile(t, sourceDir, "test.txt", "content")
	createTestFile(t, destDir, "test.txt", "content")

	calcChecksumsFor(t, sourceDir)
	calcChecksumsFor(t, destDir)

	volumeLabel := createVolumeLabel(t, destDir)

	// Manually tag the source file first
	dm, _ := core.DirectoryMapFromDir(sourceDir)
	fs, _ := dm.Get("test.txt")
	fs.AddTag(volumeLabel)
	dm.Add(fs)
	dm.Persist(sourceDir)

	// Create config
	configPath := filepath.Join(tmpDir, "config.xml")
	xc, _ := core.NewMdConfig(configPath)
	xc.AddSourceDirectory(sourceDir, "test-source")
	xc.WriteXmlCfg()

	// Run discovery
	var out bytes.Buffer
	cfg := Config{
		SourceDirs:     []string{sourceDir},
		DestinationDir: destDir,
		ConfigPath:     configPath,
		Stdout:         &out,
		XMLConfig:      xc,
		DryRun:         false,
	}

	exitCode, _ := Run(cfg)
	if exitCode != cli.ExitOk {
		t.Fatalf("Expected exit code %d, got %d", cli.ExitOk, exitCode)
	}

	// Verify counts - matched but not updated (already tagged)
	output := out.String()
	if !strings.Contains(output, "Files matched: 1") {
		t.Errorf("Expected 1 match, got: %s", output)
	}
	if !strings.Contains(output, "Metadata updated: 0") {
		t.Errorf("Expected 0 updates (already tagged), got: %s", output)
	}
}

func TestDiscoverSubdirectories(t *testing.T) {
	// Create temporary directories with subdirectories
	tmpDir := t.TempDir()
	sourceDir := filepath.Join(tmpDir, "source")
	destDir := filepath.Join(tmpDir, "dest")

	sourceSubdir := filepath.Join(sourceDir, "subdir1", "subdir2")
	destSubdir := filepath.Join(destDir, "subdir1", "subdir2")

	if err := os.MkdirAll(sourceSubdir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(destSubdir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create identical files in subdirectories
	createTestFile(t, sourceDir, "root.txt", "root content")
	createTestFile(t, sourceSubdir, "nested.txt", "nested content")
	createTestFile(t, destDir, "root.txt", "root content")
	createTestFile(t, destSubdir, "nested.txt", "nested content")

	calcChecksumsFor(t, sourceDir)
	calcChecksumsFor(t, destDir)

	volumeLabel := createVolumeLabel(t, destDir)

	// Create config
	configPath := filepath.Join(tmpDir, "config.xml")
	xc, _ := core.NewMdConfig(configPath)
	xc.AddSourceDirectory(sourceDir, "test-source")
	xc.WriteXmlCfg()

	// Run discovery
	var out bytes.Buffer
	cfg := Config{
		SourceDirs:     []string{sourceDir},
		DestinationDir: destDir,
		ConfigPath:     configPath,
		Stdout:         &out,
		XMLConfig:      xc,
		DryRun:         false,
	}

	exitCode, _ := Run(cfg)
	if exitCode != cli.ExitOk {
		t.Fatalf("Expected exit code %d, got %d", cli.ExitOk, exitCode)
	}

	// Verify both files are tagged
	if !hasBackupDest(t, sourceDir, "root.txt", volumeLabel) {
		t.Error("root.txt should be tagged")
	}
	if !hasBackupDest(t, sourceSubdir, "nested.txt", volumeLabel) {
		t.Error("nested.txt should be tagged")
	}

	output := out.String()
	if !strings.Contains(output, "Files matched: 2") {
		t.Errorf("Expected 2 matches, got: %s", output)
	}
}

func TestDiscoverNoVolumeLabel(t *testing.T) {
	// Create temporary directories
	tmpDir := t.TempDir()
	sourceDir := filepath.Join(tmpDir, "source")
	destDir := filepath.Join(tmpDir, "dest")

	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Don't create volume label for destination

	// Create config
	configPath := filepath.Join(tmpDir, "config.xml")
	xc, _ := core.NewMdConfig(configPath)
	xc.AddSourceDirectory(sourceDir, "test-source")
	xc.WriteXmlCfg()

	// Run discovery - should fail
	var out bytes.Buffer
	cfg := Config{
		SourceDirs:     []string{sourceDir},
		DestinationDir: destDir,
		ConfigPath:     configPath,
		Stdout:         &out,
		XMLConfig:      xc,
		DryRun:         false,
	}

	exitCode, err := Run(cfg)
	if exitCode == cli.ExitOk {
		t.Error("Expected non-zero exit code when volume label missing")
	}
	if err == nil {
		t.Error("Expected error when volume label missing")
	}
	if !strings.Contains(err.Error(), "mdlabel create") {
		t.Errorf("Error should mention mdlabel create, got: %v", err)
	}
}

func TestDiscoverMultipleSources(t *testing.T) {
	// Create temporary directories
	tmpDir := t.TempDir()
	source1 := filepath.Join(tmpDir, "source1")
	source2 := filepath.Join(tmpDir, "source2")
	destDir := filepath.Join(tmpDir, "dest")

	for _, dir := range []string{source1, source2, destDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	// Create files in multiple sources
	createTestFile(t, source1, "file1.txt", "content1")
	createTestFile(t, source2, "file2.txt", "content2")
	createTestFile(t, destDir, "file1.txt", "content1")
	createTestFile(t, destDir, "file2.txt", "content2")

	calcChecksumsFor(t, source1)
	calcChecksumsFor(t, source2)
	calcChecksumsFor(t, destDir)

	volumeLabel := createVolumeLabel(t, destDir)

	// Create config with multiple sources
	configPath := filepath.Join(tmpDir, "config.xml")
	xc, _ := core.NewMdConfig(configPath)
	xc.AddSourceDirectory(source1, "source1")
	xc.AddSourceDirectory(source2, "source2")
	xc.WriteXmlCfg()

	// Run discovery
	var out bytes.Buffer
	cfg := Config{
		SourceDirs:     []string{source1, source2},
		DestinationDir: destDir,
		ConfigPath:     configPath,
		Stdout:         &out,
		XMLConfig:      xc,
		DryRun:         false,
	}

	exitCode, _ := Run(cfg)
	if exitCode != cli.ExitOk {
		t.Fatalf("Expected exit code %d, got %d", cli.ExitOk, exitCode)
	}

	// Verify both files from different sources are tagged
	if !hasBackupDest(t, source1, "file1.txt", volumeLabel) {
		t.Error("file1.txt should be tagged")
	}
	if !hasBackupDest(t, source2, "file2.txt", volumeLabel) {
		t.Error("file2.txt should be tagged")
	}

	output := out.String()
	if !strings.Contains(output, "Files matched: 2") {
		t.Errorf("Expected 2 matches, got: %s", output)
	}
}
