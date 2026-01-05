package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cbehopkins/medorg/pkg/cli"
	"github.com/cbehopkins/medorg/pkg/core"
)

// Unit tests for Run() function edge cases and error paths

// TestRunWithMissingProjectConfig tests Run with nil ProjectConfig
func TestRunWithMissingProjectConfig(t *testing.T) {
	var buf bytes.Buffer

	cfg := Config{
		ProjectConfig: nil, // Missing config
		Destination:   "/tmp/dst",
		Sources:        []string{"/tmp/src"},
		LogOutput:      &buf,
		MessageWriter:  &buf,
	}

	exitCode, err := Run(cfg)
	if exitCode != cli.ExitNoConfig {
		t.Errorf("Expected exit code %d, got %d", cli.ExitNoConfig, exitCode)
	}
	if err == nil {
		t.Error("Expected error for missing config")
	}
}

// TestRunWithMissingDestination tests Run with empty destination
func TestRunWithMissingDestination(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{
		ProjectConfig: &core.MdConfig{}, // Minimal config, no file I/O
		Destination:   "",                // Empty destination
		Sources:        []string{"/tmp"}, // String only, doesn't need to exist
		LogOutput:      &buf,
		MessageWriter:  &buf,
	}

	exitCode, err := Run(cfg)
	if exitCode != cli.ExitTwoDirectoriesOnly {
		t.Errorf("Expected exit code %d, got %d", cli.ExitTwoDirectoriesOnly, exitCode)
	}
	if err == nil {
		t.Error("Expected error for missing destination")
	}
}

// TestRunWithMissingSources tests Run with empty sources
func TestRunWithMissingSources(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{
		ProjectConfig: &core.MdConfig{}, // Minimal config, no file I/O
		Destination:   "/tmp",           // String only, doesn't need to exist
		Sources:        []string{},      // Empty sources
		LogOutput:      &buf,
		MessageWriter:  &buf,
	}

	exitCode, err := Run(cfg)
	if exitCode != cli.ExitTwoDirectoriesOnly {
		t.Errorf("Expected exit code %d, got %d", cli.ExitTwoDirectoriesOnly, exitCode)
	}
	if err == nil {
		t.Error("Expected error for missing sources")
	}
}

// TestRunStatsMode tests Run with StatsMode=true
func TestRunStatsMode(t *testing.T) {
	tmpDir := t.TempDir()
	xc := newXMLCfgAt(t, tmpDir)
	setupVolumeConfigs(t, xc, tmpDir)

	// Create a test file in the directory
	testFile := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("test content"), 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	var logBuf, msgBuf bytes.Buffer
	cfg := Config{
		ProjectConfig: xc,
		Destination:   tmpDir,
		Sources:        []string{tmpDir},
		StatsMode:      true,
		LogOutput:      &logBuf,
		MessageWriter:  &msgBuf,
	}

	exitCode, err := Run(cfg)
	if exitCode != cli.ExitOk {
		t.Errorf("Stats mode failed: exit=%d err=%v", exitCode, err)
	}

	// Check that stats were generated
	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "requires") {
		t.Errorf("Expected stats output, got: %s", logOutput)
	}
}

// TestRunStatsModeWithProgressBar tests StatsMode with UseProgressBar=true
// Note: This test is skipped in non-terminal environments as progress bars require terminal access
func TestRunStatsModeWithProgressBar(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping progress bar test in CI environment")
	}

	tmpDir := t.TempDir()
	xc := newXMLCfgAt(t, tmpDir)
	setupVolumeConfigs(t, xc, tmpDir)

	var logBuf, msgBuf bytes.Buffer
	cfg := Config{
		ProjectConfig:  xc,
		Destination:    tmpDir,
		Sources:         []string{tmpDir},
		StatsMode:       true,
		UseProgressBar:  true,
		LogOutput:       &logBuf,
		MessageWriter:   &msgBuf,
	}

	exitCode, err := Run(cfg)
	// May fail with progress bar error, that's okay - just verify it was attempted
	if exitCode != cli.ExitOk && exitCode != cli.ExitProgressBar {
		t.Errorf("Unexpected exit code: %d (err=%v)", exitCode, err)
	}
}

// TestRunScanMode tests Run with ScanMode=true
func TestRunScanMode(t *testing.T) {
	tmpDir := t.TempDir()
	dstDir, err := os.MkdirTemp("", "backup-dst-*")
	if err != nil {
		t.Fatalf("Failed to create dst: %v", err)
	}
	defer os.RemoveAll(dstDir)

	xc := newXMLCfgAt(t, tmpDir)
	setupVolumeConfigs(t, xc, tmpDir, dstDir)

	var logBuf, msgBuf bytes.Buffer
	cfg := Config{
		ProjectConfig: xc,
		Destination:   dstDir,
		Sources:        []string{tmpDir},
		ScanMode:       true, // Scan only, no copy
		LogOutput:      &logBuf,
		MessageWriter:  &msgBuf,
	}

	exitCode, err := Run(cfg)
	if exitCode != cli.ExitOk {
		t.Errorf("Scan mode failed: exit=%d err=%v", exitCode, err)
	}
}

// TestRunDummyModeMultipleSources tests DummyMode with multiple sources
func TestRunDummyModeMultipleSources(t *testing.T) {
	dstDir, err := os.MkdirTemp("", "backup-dst-*")
	if err != nil {
		t.Fatalf("Failed to create dst: %v", err)
	}
	defer os.RemoveAll(dstDir)

	xc := newXMLCfgAt(t, dstDir)

	// Create two source directories
	srcDirs := make([]string, 2)
	for i := 0; i < 2; i++ {
		srcDir, err := os.MkdirTemp("", "backup-src-*")
		if err != nil {
			t.Fatalf("Failed to create src%d: %v", i, err)
		}
		defer os.RemoveAll(srcDir)
		srcDirs[i] = srcDir

		// Add a test file
		if err := os.WriteFile(filepath.Join(srcDir, "test.txt"), []byte("content"), 0o644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
	}

	setupVolumeConfigs(t, xc, append(srcDirs, dstDir)...)

	var logBuf, msgBuf bytes.Buffer
	cfg := Config{
		ProjectConfig:  xc,
		Destination:    dstDir,
		Sources:         srcDirs,
		DummyMode:       true,
		UseProgressBar:  false,
		LogOutput:       &logBuf,
		MessageWriter:   &msgBuf,
	}

	exitCode, err := Run(cfg)
	if exitCode != cli.ExitOk {
		t.Errorf("Multi-source dummy mode failed: exit=%d err=%v", exitCode, err)
	}

	// Verify multi-source messages
	output := msgBuf.String()
	if !strings.Contains(output, "Multi-Source") {
		t.Errorf("Expected multi-source message, got: %s", output)
	}
}

// TestRunDummyModeDeleteSingleSource tests DummyMode delete with single source
func TestRunDummyModeDeleteSingleSource(t *testing.T) {
	srcDir, err := os.MkdirTemp("", "backup-src-*")
	if err != nil {
		t.Fatalf("Failed to create src: %v", err)
	}
	defer os.RemoveAll(srcDir)

	dstDir, err := os.MkdirTemp("", "backup-dst-*")
	if err != nil {
		t.Fatalf("Failed to create dst: %v", err)
	}
	defer os.RemoveAll(dstDir)

	xc := newXMLCfgAt(t, dstDir)
	setupVolumeConfigs(t, xc, srcDir, dstDir)

	// Add files to both directories
	if err := os.WriteFile(filepath.Join(srcDir, "test.txt"), []byte("src"), 0o644); err != nil {
		t.Fatalf("Failed to create src file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dstDir, "orphan.txt"), []byte("orphan"), 0o644); err != nil {
		t.Fatalf("Failed to create orphan file: %v", err)
	}

	var logBuf, msgBuf bytes.Buffer
	cfg := Config{
		ProjectConfig:  xc,
		Destination:    dstDir,
		Sources:         []string{srcDir},
		DummyMode:       true,
		DeleteMode:      true, // In dummy mode, should just log
		UseProgressBar:  false,
		LogOutput:       &logBuf,
		MessageWriter:   &msgBuf,
	}

	exitCode, err := Run(cfg)
	if exitCode != cli.ExitOk {
		t.Errorf("Dummy delete mode failed: exit=%d err=%v", exitCode, err)
	}

	// In dummy mode, orphaned files should be logged but not deleted
	orphanPath := filepath.Join(dstDir, "orphan.txt")
	if _, err := os.Stat(orphanPath); err != nil {
		t.Error("Orphaned file was deleted in dummy mode (should not happen)")
	}
}

// TestRunDefaultOutputs tests that Run sets default outputs when nil
func TestRunDefaultOutputs(t *testing.T) {
	tmpDir := t.TempDir()
	xc := newXMLCfgAt(t, tmpDir)
	setupVolumeConfigs(t, xc, tmpDir)

	cfg := Config{
		ProjectConfig: xc,
		Destination:   tmpDir,
		Sources:        []string{tmpDir},
		DummyMode:      true,
		LogOutput:      nil, // Will default to os.Stderr
		MessageWriter:  nil, // Will default to os.Stdout
		ShutdownChan:   nil, // Will default to make(chan struct{})
	}

	exitCode, err := Run(cfg)
	if exitCode != cli.ExitOk {
		t.Errorf("Run with default outputs failed: exit=%d err=%v", exitCode, err)
	}
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

// TestRunWithIgnorePatterns tests that ignore patterns are respected
func TestRunWithIgnorePatterns(t *testing.T) {
	srcDir, err := os.MkdirTemp("", "backup-src-*")
	if err != nil {
		t.Fatalf("Failed to create src: %v", err)
	}
	defer os.RemoveAll(srcDir)

	dstDir, err := os.MkdirTemp("", "backup-dst-*")
	if err != nil {
		t.Fatalf("Failed to create dst: %v", err)
	}
	defer os.RemoveAll(dstDir)

	xc := newXMLCfgAt(t, dstDir)
	setupVolumeConfigs(t, xc, srcDir, dstDir)

	// Add ignore pattern
	if len(xc.IgnorePatterns) == 0 {
		xc.IgnorePatterns = []string{"*.tmp"}
	}

	// Create files
	if err := os.WriteFile(filepath.Join(srcDir, "keep.txt"), []byte("keep"), 0o644); err != nil {
		t.Fatalf("Failed to create keep.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "ignore.tmp"), []byte("ignore"), 0o644); err != nil {
		t.Fatalf("Failed to create ignore.tmp: %v", err)
	}

	var logBuf, msgBuf bytes.Buffer
	cfg := Config{
		ProjectConfig:  xc,
		Destination:    dstDir,
		Sources:         []string{srcDir},
		DummyMode:       true,
		UseProgressBar:  false,
		LogOutput:       &logBuf,
		MessageWriter:   &msgBuf,
	}

	exitCode, err := Run(cfg)
	if exitCode != cli.ExitOk {
		t.Errorf("Run with ignore patterns failed: exit=%d err=%v", exitCode, err)
	}

	// Check that ignored files are logged as skipped
	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "Skipping (ignored)") {
		t.Logf("Expected skip message in log, got: %s", logOutput)
	}
}

// TestRunCompletionMessages tests that Run produces completion messages
func TestRunCompletionMessages(t *testing.T) {
	tmpDir := t.TempDir()
	xc := newXMLCfgAt(t, tmpDir)
	setupVolumeConfigs(t, xc, tmpDir)

	var buf bytes.Buffer
	cfg := Config{
		ProjectConfig:  xc,
		Destination:    tmpDir,
		Sources:         []string{tmpDir},
		DummyMode:      true,
		UseProgressBar:  false,
		LogOutput:       &buf,
		MessageWriter:   &buf,
	}

	exitCode, err := Run(cfg)
	if exitCode != cli.ExitOk {
		t.Fatalf("Run failed: exit=%d err=%v", exitCode, err)
	}

	output := buf.String()
	if !strings.Contains(output, "Completed") {
		t.Errorf("Expected completion message, got: %s", output)
	}
}
