package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/medorg/pkg/cli"
	"github.com/cbehopkins/medorg/pkg/core"
)

// Additional unit tests for mdlabel command coverage

// TestRunCreateCommand tests the run function with create command
func TestRunCreateCommand(t *testing.T) {
	tmpDir := t.TempDir()

	// Set command line args
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"mdlabel", "create", tmpDir}

	exitCode, err := run()
	if err != nil {
		t.Fatalf("run() failed: %v", err)
	}
	if exitCode != cli.ExitOk {
		t.Errorf("Expected exit code %d, got %d", cli.ExitOk, exitCode)
	}

	// Verify label was created
	labelFile := filepath.Join(tmpDir, ".mdbackup.xml")
	if _, err := os.Stat(labelFile); os.IsNotExist(err) {
		t.Errorf("Label file not created")
	}
}

// TestRunShowCommand tests the run function with show command
func TestRunShowCommand(t *testing.T) {
	tmpDir := t.TempDir()

	// First create a label
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"mdlabel", "create", tmpDir}

	_, _ = run()

	// Now show the label
	os.Args = []string{"mdlabel", "show", tmpDir}

	exitCode, err := run()
	if err != nil {
		t.Fatalf("run() show failed: %v", err)
	}
	if exitCode != cli.ExitOk {
		t.Errorf("Expected exit code %d, got %d", cli.ExitOk, exitCode)
	}
}

// TestRunRecreateCommand tests the run function with recreate command
func TestRunRecreateCommand(t *testing.T) {
	tmpDir := t.TempDir()

	// First create a label
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"mdlabel", "create", tmpDir}

	_, _ = run()

	// Now recreate the label
	os.Args = []string{"mdlabel", "recreate", tmpDir}

	exitCode, err := run()
	if err != nil {
		t.Fatalf("run() recreate failed: %v", err)
	}
	if exitCode != cli.ExitOk {
		t.Errorf("Expected exit code %d, got %d", cli.ExitOk, exitCode)
	}
}

// TestRunInvalidCommand tests run with invalid command
func TestRunInvalidCommand(t *testing.T) {
	tmpDir := t.TempDir()

	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"mdlabel", "invalid", tmpDir}

	exitCode, err := run()
	if exitCode != cli.ExitInvalidArgs {
		t.Errorf("Expected exit code %d for invalid command, got %d", cli.ExitInvalidArgs, exitCode)
	}
	_ = err
}

// TestRunMissingArgs tests run with insufficient arguments
func TestRunMissingArgs(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"mdlabel"}

	exitCode, _ := run()
	if exitCode != cli.ExitInvalidArgs {
		t.Errorf("Expected exit code %d for missing args, got %d", cli.ExitInvalidArgs, exitCode)
	}
}

// TestRunNonexistentPath tests run with nonexistent path
func TestRunNonexistentPath(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"mdlabel", "create", "/nonexistent/path/that/does/not/exist"}

	exitCode, _ := run()
	if exitCode != cli.ExitPathNotExist {
		t.Errorf("Expected exit code %d for nonexistent path, got %d", cli.ExitPathNotExist, exitCode)
	}
}

// TestRunShowNonexistentLabel tests show on directory without label
func TestRunShowNonexistentLabel(t *testing.T) {
	tmpDir := t.TempDir()

	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"mdlabel", "show", tmpDir}

	exitCode, _ := run()
	// Note: GetVolumeLabel creates a label if one doesn't exist, so this will succeed
	if exitCode != cli.ExitOk {
		t.Errorf("Expected exit code %d, got %d", cli.ExitOk, exitCode)
	}
}

// TestRunCreateAlreadyExists tests create on existing label
func TestRunCreateAlreadyExists(t *testing.T) {
	tmpDir := t.TempDir()

	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	// Create first label
	os.Args = []string{"mdlabel", "create", tmpDir}
	exitCode1, _ := run()
	if exitCode1 != cli.ExitOk {
		t.Fatalf("First create failed with exit code %d", exitCode1)
	}

	// Try to create again
	os.Args = []string{"mdlabel", "create", tmpDir}
	exitCode2, _ := run()
	if exitCode2 != cli.ExitConfigError {
		t.Errorf("Expected exit code %d for duplicate create, got %d", cli.ExitConfigError, exitCode2)
	}
}

// TestCreateLabelWithConfig tests creating label with explicit config
func TestCreateLabelWithConfig(t *testing.T) {
	tmpDir := t.TempDir()
	xc := &core.MdConfig{}

	err := createLabel(xc, tmpDir)
	if err != nil {
		t.Fatalf("createLabel failed: %v", err)
	}

	// Verify label is 8 characters
	label, err := xc.GetVolumeLabel(tmpDir)
	if err != nil {
		t.Fatalf("GetVolumeLabel failed: %v", err)
	}

	if len(label) != 8 {
		t.Errorf("Expected 8-character label, got %d: %s", len(label), label)
	}
}

// TestShowLabelWithConfig tests showing label with explicit config
func TestShowLabelWithConfig(t *testing.T) {
	tmpDir := t.TempDir()
	xc := &core.MdConfig{}

	// Create label first
	if err := createLabel(xc, tmpDir); err != nil {
		t.Fatalf("createLabel failed: %v", err)
	}

	// Show label
	if err := showLabel(xc, tmpDir); err != nil {
		t.Fatalf("showLabel failed: %v", err)
	}
}

// TestRecreateLabelWithConfig tests recreating label
func TestRecreateLabelWithConfig(t *testing.T) {
	tmpDir := t.TempDir()
	xc := &core.MdConfig{}

	// Create label first
	if err := createLabel(xc, tmpDir); err != nil {
		t.Fatalf("createLabel failed: %v", err)
	}

	originalLabel, err := xc.GetVolumeLabel(tmpDir)
	if err != nil {
		t.Fatalf("GetVolumeLabel failed: %v", err)
	}

	// Recreate label
	if err := recreateLabel(xc, tmpDir); err != nil {
		t.Fatalf("recreateLabel failed: %v", err)
	}

	// New label should be different
	xc2 := &core.MdConfig{}
	newLabel, err := xc2.GetVolumeLabel(tmpDir)
	if err != nil {
		t.Fatalf("GetVolumeLabel after recreate failed: %v", err)
	}

	if originalLabel == newLabel {
		t.Errorf("Expected new label to be different from original")
	}
}

// TestLabelInSubdirectory tests showing label from subdirectory
func TestLabelInSubdirectory(t *testing.T) {
	tmpDir := t.TempDir()
	subDir := filepath.Join(tmpDir, "sub", "dir")
	if err := os.MkdirAll(subDir, 0o755); err != nil {
		t.Fatalf("Failed to create subdirectory: %v", err)
	}

	xc := &core.MdConfig{}

	// Create label in root
	if err := createLabel(xc, tmpDir); err != nil {
		t.Fatalf("createLabel failed: %v", err)
	}

	// Show from subdirectory (should find label in parent)
	err := showLabel(xc, subDir)
	if err != nil {
		t.Fatalf("showLabel from subdirectory failed: %v", err)
	}
}
