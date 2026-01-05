package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

// Unit tests for mdsource main logic (additional coverage)

// TestRunWithAddCommand tests adding a source directory
func TestRunWithAddCommand(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.xml")
	srcPath := filepath.Join(tmpDir, "source")
	os.MkdirAll(srcPath, 0o755)

	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	// Create a source with add command
	var buf bytes.Buffer
	os.Args = []string{"mdsource", "add", "-config", configPath, "-path", srcPath, "-alias", "test"}

	exitCode := run(&buf)
	if exitCode != 0 {
		t.Errorf("Expected exit code 0 for add, got %d", exitCode)
	}

	// Verify the config was created and contains the source
	xc, err := core.LoadOrCreateMdConfigWithPath(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	sources := xc.GetSourcePaths()
	if len(sources) == 0 {
		t.Error("Expected source to be added")
	}
}

// TestRunWithListCommand tests listing source directories
func TestRunWithListCommand(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.xml")
	srcPath := filepath.Join(tmpDir, "source")
	os.MkdirAll(srcPath, 0o755)

	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	// First add a source
	os.Args = []string{"mdsource", "add", "-config", configPath, "-path", srcPath, "-alias", "test"}
	run(bytes.NewBuffer(nil))

	// Now list sources
	var buf bytes.Buffer
	os.Args = []string{"mdsource", "list", "-config", configPath}

	exitCode := run(&buf)
	if exitCode != 0 {
		t.Errorf("Expected exit code 0 for list, got %d", exitCode)
	}

	output := buf.String()
	if output == "" {
		t.Error("Expected list output")
	}
}

// TestRunWithRemoveCommand tests removing a source directory
func TestRunWithRemoveCommand(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.xml")
	srcPath := filepath.Join(tmpDir, "source")
	os.MkdirAll(srcPath, 0o755)

	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	// First add a source
	os.Args = []string{"mdsource", "add", "-config", configPath, "-path", srcPath, "-alias", "test"}
	run(bytes.NewBuffer(nil))

	// Now remove it
	var buf bytes.Buffer
	os.Args = []string{"mdsource", "remove", "-config", configPath, "-alias", "test"}

	exitCode := run(&buf)
	if exitCode != 0 {
		t.Errorf("Expected exit code 0 for remove, got %d", exitCode)
	}
}

// TestRunWithRestoreCommand tests configuring restore destination
func TestRunWithRestoreCommand(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.xml")
	srcPath := filepath.Join(tmpDir, "source")
	dstPath := filepath.Join(tmpDir, "destination")
	os.MkdirAll(srcPath, 0o755)
	os.MkdirAll(dstPath, 0o755)

	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	// First add a source
	os.Args = []string{"mdsource", "add", "-config", configPath, "-path", srcPath, "-alias", "test"}
	run(bytes.NewBuffer(nil))

	// Now set restore destination
	var buf bytes.Buffer
	os.Args = []string{"mdsource", "restore", "-config", configPath, "-alias", "test", "-path", dstPath}

	exitCode := run(&buf)
	if exitCode != 0 {
		t.Errorf("Expected exit code 0 for restore, got %d", exitCode)
	}
}

// TestRunWithIgnoreAddCommand tests adding ignore pattern
func TestRunWithIgnoreAddCommand(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.xml")

	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	var buf bytes.Buffer
	os.Args = []string{"mdsource", "ignore-add", "-config", configPath, "-pattern", ".*\\.tmp$"}

	exitCode := run(&buf)
	if exitCode != 0 {
		t.Errorf("Expected exit code 0 for ignore-add, got %d", exitCode)
	}

	// Verify pattern was added
	xc, err := core.LoadOrCreateMdConfigWithPath(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if len(xc.IgnorePatterns) == 0 {
		t.Error("Expected ignore pattern to be added")
	}
}

// TestRunWithIgnoreTestCommand tests testing ignore patterns
func TestRunWithIgnoreTestCommand(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.xml")
	testFile := filepath.Join(tmpDir, "test.tmp")

	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	// Add ignore pattern
	os.Args = []string{"mdsource", "ignore-add", "-config", configPath, "-pattern", ".*\\.tmp$"}
	run(bytes.NewBuffer(nil))

	// Test the pattern
	var buf bytes.Buffer
	os.Args = []string{"mdsource", "ignore-test", "-config", configPath, "-path", testFile}

	exitCode := run(&buf)
	if exitCode != 0 {
		t.Errorf("Expected exit code 0 for ignore-test, got %d", exitCode)
	}

	output := buf.String()
	if output == "" {
		t.Error("Expected ignore-test output")
	}
}

// TestRunWithNoCommand tests mdsource with no command
func TestRunWithNoCommand(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	os.Args = []string{"mdsource"}

	var buf bytes.Buffer
	exitCode := run(&buf)
	// Should print usage and return non-zero exit code
	if exitCode == 0 {
		t.Errorf("Expected non-zero exit code for no command, got %d", exitCode)
	}
}

// TestRunWithInvalidCommand tests mdsource with invalid command
func TestRunWithInvalidCommand(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	os.Args = []string{"mdsource", "invalid-command"}

	var buf bytes.Buffer
	exitCode := run(&buf)
	if exitCode == 0 {
		t.Errorf("Expected non-zero exit code for invalid command, got %d", exitCode)
	}
}

// TestRunAddWithInvalidPath tests add command with nonexistent path
func TestRunAddWithInvalidPath(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.xml")

	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	os.Args = []string{"mdsource", "add", "-config", configPath, "-path", "/nonexistent/path", "-alias", "test"}

	var buf bytes.Buffer
	exitCode := run(&buf)
	// Should fail because path doesn't exist
	if exitCode == 0 {
		t.Errorf("Expected non-zero exit code for nonexistent path, got %d", exitCode)
	}
}

// TestRunAddWithoutAlias tests add command without alias
func TestRunAddWithoutAlias(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.xml")
	srcPath := filepath.Join(tmpDir, "source")
	os.MkdirAll(srcPath, 0o755)

	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	os.Args = []string{"mdsource", "add", "-config", configPath, "-path", srcPath}

	var buf bytes.Buffer
	exitCode := run(&buf)
	// Should handle missing alias gracefully
	if exitCode == 0 {
		t.Errorf("Expected non-zero exit code for missing alias, got %d", exitCode)
	}
}

// TestRunRemoveNonexistentAlias tests removing nonexistent alias
func TestRunRemoveNonexistentAlias(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.xml")

	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	os.Args = []string{"mdsource", "remove", "-config", configPath, "-alias", "nonexistent"}

	var buf bytes.Buffer
	exitCode := run(&buf)
	// Should fail because alias doesn't exist
	if exitCode == 0 {
		t.Errorf("Expected non-zero exit code for nonexistent alias, got %d", exitCode)
	}
}
