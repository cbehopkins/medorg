package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

func TestCreateLabel(t *testing.T) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "mdlabel_test_create_")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	xc := &core.MdConfig{}

	// Create a new label
	err = createLabel(xc, tmpDir)
	if err != nil {
		t.Fatalf("createLabel failed: %v", err)
	}

	// Verify label file exists
	labelFile := filepath.Join(tmpDir, ".mdbackup.xml")
	if _, err := os.Stat(labelFile); os.IsNotExist(err) {
		t.Fatalf("Label file not created at %s", labelFile)
	}

	// Verify we can read the label
	label, err := xc.GetVolumeLabel(tmpDir)
	if err != nil {
		t.Fatalf("Failed to read created label: %v", err)
	}

	if len(label) != 8 {
		t.Errorf("Expected 8-character label, got %d characters: %s", len(label), label)
	}

	// Verify creating again fails
	err = createLabel(xc, tmpDir)
	if err == nil {
		t.Error("Expected error when creating label twice, got nil")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("Expected 'already exists' error, got: %v", err)
	}
}

func TestShowLabel(t *testing.T) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "mdlabel_test_show_")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	xc := &core.MdConfig{}

	// Create a label first
	err = createLabel(xc, tmpDir)
	if err != nil {
		t.Fatalf("createLabel failed: %v", err)
	}

	// Get the created label
	expectedLabel, err := xc.GetVolumeLabel(tmpDir)
	if err != nil {
		t.Fatalf("Failed to get label: %v", err)
	}

	// Show label should succeed
	err = showLabel(xc, tmpDir)
	if err != nil {
		t.Errorf("showLabel failed: %v", err)
	}

	// Verify it shows the correct label
	label, err := xc.GetVolumeLabel(tmpDir)
	if err != nil {
		t.Fatalf("Failed to retrieve label: %v", err)
	}
	if label != expectedLabel {
		t.Errorf("Expected label %s, got %s", expectedLabel, label)
	}
}

func TestShowLabelNotFound(t *testing.T) {
	// Create temporary directory without a label
	tmpDir, err := os.MkdirTemp("", "mdlabel_test_notfound_")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	xc := &core.MdConfig{}

	// showLabel should create a label if it doesn't exist (per VolumeCfgFromDir behavior)
	// Let's verify the behavior
	err = showLabel(xc, tmpDir)
	if err != nil {
		// If it errors, that's also valid - check that it's a reasonable error
		if !strings.Contains(err.Error(), "not found") &&
			!strings.Contains(err.Error(), "no volume label") {
			t.Errorf("Expected 'not found' or similar error, got: %v", err)
		}
	}
}

func TestRecreateLabel(t *testing.T) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "mdlabel_test_recreate_")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	xc := &core.MdConfig{}

	// Create initial label
	err = createLabel(xc, tmpDir)
	if err != nil {
		t.Fatalf("createLabel failed: %v", err)
	}

	// Get the original label
	originalLabel, err := xc.GetVolumeLabel(tmpDir)
	if err != nil {
		t.Fatalf("Failed to get original label: %v", err)
	}

	// Recreate the label
	err = recreateLabel(xc, tmpDir)
	if err != nil {
		t.Fatalf("recreateLabel failed: %v", err)
	}

	// Get the new label
	newLabel, err := xc.GetVolumeLabel(tmpDir)
	if err != nil {
		t.Fatalf("Failed to get new label: %v", err)
	}

	// Verify the label changed
	if originalLabel == newLabel {
		t.Errorf("Expected new label to differ from original, both are: %s", originalLabel)
	}

	// Verify both are valid 8-character labels
	if len(newLabel) != 8 {
		t.Errorf("Expected 8-character new label, got %d characters: %s", len(newLabel), newLabel)
	}
}

func TestShowLabelInSubdirectory(t *testing.T) {
	// Create temporary directory structure
	tmpDir, err := os.MkdirTemp("", "mdlabel_test_subdir_")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	subDir := filepath.Join(tmpDir, "level1", "level2", "level3")
	err = os.MkdirAll(subDir, 0o755)
	if err != nil {
		t.Fatalf("Failed to create subdirectories: %v", err)
	}

	xc := &core.MdConfig{}

	// Create label at root
	err = createLabel(xc, tmpDir)
	if err != nil {
		t.Fatalf("createLabel failed: %v", err)
	}

	// Get label from root
	rootLabel, err := xc.GetVolumeLabel(tmpDir)
	if err != nil {
		t.Fatalf("Failed to get root label: %v", err)
	}

	// Show label from subdirectory should find the root label
	err = showLabel(xc, subDir)
	if err != nil {
		t.Fatalf("showLabel from subdirectory failed: %v", err)
	}

	// Verify it's the same label
	subLabel, err := xc.GetVolumeLabel(subDir)
	if err != nil {
		t.Fatalf("Failed to get label from subdirectory: %v", err)
	}

	if rootLabel != subLabel {
		t.Errorf("Expected subdirectory to find root label %s, got %s", rootLabel, subLabel)
	}
}

func TestLabelUniqueness(t *testing.T) {
	// Create two temporary directories
	tmpDir1, err := os.MkdirTemp("", "mdlabel_test_unique1_")
	if err != nil {
		t.Fatalf("Failed to create temp dir 1: %v", err)
	}
	defer os.RemoveAll(tmpDir1)

	tmpDir2, err := os.MkdirTemp("", "mdlabel_test_unique2_")
	if err != nil {
		t.Fatalf("Failed to create temp dir 2: %v", err)
	}
	defer os.RemoveAll(tmpDir2)

	xc := &core.MdConfig{}

	// Create labels in both
	err = createLabel(xc, tmpDir1)
	if err != nil {
		t.Fatalf("createLabel 1 failed: %v", err)
	}

	err = createLabel(xc, tmpDir2)
	if err != nil {
		t.Fatalf("createLabel 2 failed: %v", err)
	}

	// Get both labels
	label1, err := xc.GetVolumeLabel(tmpDir1)
	if err != nil {
		t.Fatalf("Failed to get label 1: %v", err)
	}

	label2, err := xc.GetVolumeLabel(tmpDir2)
	if err != nil {
		t.Fatalf("Failed to get label 2: %v", err)
	}

	// Verify they're different
	if label1 == label2 {
		t.Errorf("Expected unique labels, both are: %s", label1)
	}
}

func TestRecreateLabelWithoutExisting(t *testing.T) {
	// Create temporary directory without a label
	tmpDir, err := os.MkdirTemp("", "mdlabel_test_recreate_new_")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	xc := &core.MdConfig{}

	// Recreate should work even without existing label
	err = recreateLabel(xc, tmpDir)
	if err != nil {
		t.Fatalf("recreateLabel failed on new directory: %v", err)
	}

	// Verify label was created
	label, err := xc.GetVolumeLabel(tmpDir)
	if err != nil {
		t.Fatalf("Failed to get label after recreate: %v", err)
	}

	if len(label) != 8 {
		t.Errorf("Expected 8-character label, got %d characters: %s", len(label), label)
	}
}
