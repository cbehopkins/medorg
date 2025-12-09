package core

import (
	"os"
	"path/filepath"
	"testing"
)

// TestNewMdConfig tests loading and creating config from file
func TestNewMdConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, ConfigFileName)

	// Test creating new config (file doesn't exist)
	cfg, err := NewMdConfig(configPath)
	if err != nil {
		t.Fatalf("NewMdConfig failed: %v", err)
	}
	if cfg == nil {
		t.Fatal("NewMdConfig returned nil config")
	}
	if cfg.fn != configPath {
		t.Errorf("Config filename: got %q, want %q", cfg.fn, configPath)
	}

	// Add some data and save
	cfg.AddLabel("test-volume")
	if err := cfg.WriteXmlCfg(); err != nil {
		t.Fatalf("WriteXmlCfg failed: %v", err)
	}

	// Test loading existing config
	cfg2, err := NewMdConfig(configPath)
	if err != nil {
		t.Fatalf("NewMdConfig on existing file failed: %v", err)
	}
	if !cfg2.HasLabel("test-volume") {
		t.Error("Expected loaded config to have test-volume label")
	}
}

// TestMdConfigVolumeLabels tests volume label management
func TestMdConfigVolumeLabels(t *testing.T) {
	cfg := &MdConfig{}

	// Test adding labels
	if !cfg.AddLabel("volume1") {
		t.Error("AddLabel should return true for new label")
	}
	if !cfg.AddLabel("volume2") {
		t.Error("AddLabel should return true for new label")
	}

	// Test adding duplicate
	if cfg.AddLabel("volume1") {
		t.Error("AddLabel should return false for duplicate label")
	}

	// Test HasLabel
	if !cfg.HasLabel("volume1") {
		t.Error("HasLabel should return true for existing label")
	}
	if !cfg.HasLabel("volume2") {
		t.Error("HasLabel should return true for existing label")
	}
	if cfg.HasLabel("volume3") {
		t.Error("HasLabel should return false for non-existent label")
	}

	// Verify labels are stored
	if len(cfg.VolumeLabels) != 2 {
		t.Errorf("Expected 2 labels, got %d", len(cfg.VolumeLabels))
	}
}

// TestMdConfigSourceDirectories tests source directory management
func TestMdConfigSourceDirectories(t *testing.T) {
	cfg := &MdConfig{}

	// Test adding source directories
	if !cfg.AddSourceDirectory("/path/to/docs", "docs") {
		t.Error("AddSourceDirectory should return true for new directory")
	}
	if !cfg.AddSourceDirectory("/path/to/photos", "photos") {
		t.Error("AddSourceDirectory should return true for new directory")
	}

	// Test adding duplicate alias
	if cfg.AddSourceDirectory("/different/path", "docs") {
		t.Error("AddSourceDirectory should return false for duplicate alias")
	}

	// Test HasSourceDirectory
	if !cfg.HasSourceDirectory("docs") {
		t.Error("HasSourceDirectory should return true for existing alias")
	}
	if cfg.HasSourceDirectory("videos") {
		t.Error("HasSourceDirectory should return false for non-existent alias")
	}

	// Test GetSourceDirectory
	sd, ok := cfg.GetSourceDirectory("docs")
	if !ok {
		t.Error("GetSourceDirectory should return true for existing alias")
	}
	if sd.Alias != "docs" {
		t.Errorf("Alias: got %q, want 'docs'", sd.Alias)
	}
	expectedPath := filepath.Clean("/path/to/docs")
	if sd.Path != expectedPath {
		t.Errorf("Path: got %q, want %q", sd.Path, expectedPath)
	}

	// Test GetSourceDirectory for non-existent
	_, ok = cfg.GetSourceDirectory("videos")
	if ok {
		t.Error("GetSourceDirectory should return false for non-existent alias")
	}

	// Test RemoveSourceDirectory
	if !cfg.RemoveSourceDirectory("docs") {
		t.Error("RemoveSourceDirectory should return true for existing alias")
	}
	if cfg.HasSourceDirectory("docs") {
		t.Error("Source directory should be removed")
	}
	if cfg.RemoveSourceDirectory("docs") {
		t.Error("RemoveSourceDirectory should return false for non-existent alias")
	}
}

// TestMdConfigGetSourcePaths tests getting all source paths
func TestMdConfigGetSourcePaths(t *testing.T) {
	cfg := &MdConfig{}

	// Empty config
	paths := cfg.GetSourcePaths()
	if len(paths) != 0 {
		t.Errorf("Expected 0 paths, got %d", len(paths))
	}

	// Add some directories
	cfg.AddSourceDirectory("/path/one", "one")
	cfg.AddSourceDirectory("/path/two", "two")
	cfg.AddSourceDirectory("/path/three", "three")

	paths = cfg.GetSourcePaths()
	if len(paths) != 3 {
		t.Errorf("Expected 3 paths, got %d", len(paths))
	}

	// Verify paths are correct (order might vary)
	expectedPaths := map[string]bool{
		filepath.Clean("/path/one"):   false,
		filepath.Clean("/path/two"):   false,
		filepath.Clean("/path/three"): false,
	}
	for _, path := range paths {
		if _, exists := expectedPaths[path]; !exists {
			t.Errorf("Unexpected path: %q", path)
		}
		expectedPaths[path] = true
	}
	for path, found := range expectedPaths {
		if !found {
			t.Errorf("Expected path not found: %q", path)
		}
	}
}

// TestMdConfigGetAliasForPath tests reverse lookup of alias by path
func TestMdConfigGetAliasForPath(t *testing.T) {
	cfg := &MdConfig{}

	cfg.AddSourceDirectory("/path/to/docs", "docs")
	cfg.AddSourceDirectory("/path/to/photos", "photos")

	tests := []struct {
		path      string
		wantAlias string
	}{
		{"/path/to/docs", "docs"},
		{"/path/to/photos", "photos"},
		{"/path/to/videos", ""},    // not found
		{"/path/to/docs/", "docs"}, // with trailing slash (should be cleaned)
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			alias := cfg.GetAliasForPath(tt.path)
			if alias != tt.wantAlias {
				t.Errorf("GetAliasForPath(%q) = %q, want %q", tt.path, alias, tt.wantAlias)
			}
		})
	}
}

// TestMdConfigRestoreDestinations tests restore destination management
func TestMdConfigRestoreDestinations(t *testing.T) {
	cfg := &MdConfig{}

	// First add source directories
	cfg.AddSourceDirectory("/backup/src/docs", "docs")
	cfg.AddSourceDirectory("/backup/src/photos", "photos")

	// Test setting restore destination with explicit path
	err := cfg.SetRestoreDestination("docs", "/restore/docs")
	if err != nil {
		t.Errorf("SetRestoreDestination failed: %v", err)
	}

	// Test getting restore destination
	path, ok := cfg.GetRestoreDestination("docs")
	if !ok {
		t.Error("GetRestoreDestination should return true for existing alias")
	}
	expectedPath := filepath.Clean("/restore/docs")
	if path != expectedPath {
		t.Errorf("Path: got %q, want %q", path, expectedPath)
	}

	// Test setting restore destination with empty path (should use source path)
	err = cfg.SetRestoreDestination("photos", "")
	if err != nil {
		t.Errorf("SetRestoreDestination with empty path failed: %v", err)
	}

	path, ok = cfg.GetRestoreDestination("photos")
	if !ok {
		t.Error("GetRestoreDestination should return true")
	}
	expectedPath = filepath.Clean("/backup/src/photos")
	if path != expectedPath {
		t.Errorf("Path: got %q, want %q (should use source path)", path, expectedPath)
	}

	// Test updating existing restore destination
	err = cfg.SetRestoreDestination("docs", "/new/restore/docs")
	if err != nil {
		t.Errorf("SetRestoreDestination update failed: %v", err)
	}

	path, _ = cfg.GetRestoreDestination("docs")
	expectedPath = filepath.Clean("/new/restore/docs")
	if path != expectedPath {
		t.Errorf("Updated path: got %q, want %q", path, expectedPath)
	}

	// Test setting destination for non-existent source with empty path
	err = cfg.SetRestoreDestination("videos", "")
	if err == nil {
		t.Error("SetRestoreDestination should fail for non-existent source with empty path")
	}

	// Test removing restore destination
	if !cfg.RemoveRestoreDestination("docs") {
		t.Error("RemoveRestoreDestination should return true for existing alias")
	}
	if _, ok := cfg.GetRestoreDestination("docs"); ok {
		t.Error("Restore destination should be removed")
	}
	if cfg.RemoveRestoreDestination("docs") {
		t.Error("RemoveRestoreDestination should return false for non-existent alias")
	}
}

// TestMdConfigXMLSerialization tests XML marshaling and unmarshaling
func TestMdConfigXMLSerialization(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.xml")

	// Create config with data
	cfg := &MdConfig{fn: configPath}
	cfg.AddLabel("volume1")
	cfg.AddLabel("volume2")
	cfg.AddSourceDirectory("/src/docs", "docs")
	cfg.AddSourceDirectory("/src/photos", "photos")
	cfg.SetRestoreDestination("docs", "/restore/docs")

	// Write to file
	if err := cfg.WriteXmlCfg(); err != nil {
		t.Fatalf("WriteXmlCfg failed: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Fatal("Config file was not created")
	}

	// Load from file
	cfg2, err := NewMdConfig(configPath)
	if err != nil {
		t.Fatalf("NewMdConfig failed: %v", err)
	}

	// Verify volume labels
	if !cfg2.HasLabel("volume1") || !cfg2.HasLabel("volume2") {
		t.Error("Volume labels not preserved after load")
	}

	// Verify source directories
	if !cfg2.HasSourceDirectory("docs") || !cfg2.HasSourceDirectory("photos") {
		t.Error("Source directories not preserved after load")
	}

	// Verify restore destinations
	path, ok := cfg2.GetRestoreDestination("docs")
	if !ok {
		t.Error("Restore destination not preserved after load")
	}
	expectedPath := filepath.Clean("/restore/docs")
	if path != expectedPath {
		t.Errorf("Restore destination path: got %q, want %q", path, expectedPath)
	}

	// Verify source directory paths
	sd, _ := cfg2.GetSourceDirectory("docs")
	expectedSrcPath := filepath.Clean("/src/docs")
	if sd.Path != expectedSrcPath {
		t.Errorf("Source directory path: got %q, want %q", sd.Path, expectedSrcPath)
	}
}

// TestMdConfigFromXML tests unmarshaling from XML bytes
func TestMdConfigFromXML(t *testing.T) {
	xmlData := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<xc>
  <vl>volume1</vl>
  <vl>volume2</vl>
  <src path="/backup/docs" alias="docs"></src>
  <src path="/backup/photos" alias="photos"></src>
  <restore alias="docs" path="/restore/docs"></restore>
</xc>`)

	cfg := &MdConfig{}
	err := cfg.FromXML(xmlData)
	if err != nil {
		t.Fatalf("FromXML failed: %v", err)
	}

	// Verify volume labels
	if len(cfg.VolumeLabels) != 2 {
		t.Errorf("Expected 2 volume labels, got %d", len(cfg.VolumeLabels))
	}
	if !cfg.HasLabel("volume1") || !cfg.HasLabel("volume2") {
		t.Error("Volume labels not correctly unmarshaled")
	}

	// Verify source directories
	if len(cfg.SourceDirectories) != 2 {
		t.Errorf("Expected 2 source directories, got %d", len(cfg.SourceDirectories))
	}

	// Verify restore destinations
	if len(cfg.RestoreDestinations) != 1 {
		t.Errorf("Expected 1 restore destination, got %d", len(cfg.RestoreDestinations))
	}
}

// TestMdConfigEmptyXML tests handling of empty/missing XML
func TestMdConfigEmptyXML(t *testing.T) {
	cfg := &MdConfig{}

	// Test with empty XML
	err := cfg.FromXML([]byte(""))
	if err != nil {
		t.Errorf("FromXML with empty data should not error, got: %v", err)
	}
}

// TestLoadOrCreateMdConfigWithPath tests the load/create helper
func TestLoadOrCreateMdConfigWithPath(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "test-config.xml")

	// Test creating new config
	cfg, err := LoadOrCreateMdConfigWithPath(configPath)
	if err != nil {
		t.Fatalf("LoadOrCreateMdConfigWithPath failed: %v", err)
	}
	if cfg == nil {
		t.Fatal("Config is nil")
	}

	// Add data and save
	cfg.AddLabel("test-label")
	if err := cfg.WriteXmlCfg(); err != nil {
		t.Fatalf("WriteXmlCfg failed: %v", err)
	}

	// Test loading existing config
	cfg2, err := LoadOrCreateMdConfigWithPath(configPath)
	if err != nil {
		t.Fatalf("LoadOrCreateMdConfigWithPath on existing file failed: %v", err)
	}
	if !cfg2.HasLabel("test-label") {
		t.Error("Loaded config should have test-label")
	}
}

// TestMdConfigPathCleaning tests that paths are cleaned consistently
func TestMdConfigPathCleaning(t *testing.T) {
	cfg := &MdConfig{}

	// Add with trailing slashes and dots
	cfg.AddSourceDirectory("/path/to/dir/", "test1")
	cfg.AddSourceDirectory("/path/to/../to/dir", "test2")

	// Both should be cleaned to same path
	sd1, _ := cfg.GetSourceDirectory("test1")
	sd2, _ := cfg.GetSourceDirectory("test2")

	expected := filepath.Clean("/path/to/dir")
	if sd1.Path != expected {
		t.Errorf("Path1 not cleaned: got %q, want %q", sd1.Path, expected)
	}
	if sd2.Path != expected {
		t.Errorf("Path2 not cleaned: got %q, want %q", sd2.Path, expected)
	}

	// Test restore destination path cleaning
	cfg.SetRestoreDestination("test1", "/restore/path/")
	path, _ := cfg.GetRestoreDestination("test1")
	expectedRestore := filepath.Clean("/restore/path")
	if path != expectedRestore {
		t.Errorf("Restore path not cleaned: got %q, want %q", path, expectedRestore)
	}
}

// TestMdConfigComplexWorkflow tests a realistic workflow
func TestMdConfigComplexWorkflow(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, ConfigFileName)

	// Create and configure
	cfg, _ := NewMdConfig(configPath)

	// Add backup volumes
	cfg.AddLabel("backup-vol-1")
	cfg.AddLabel("backup-vol-2")

	// Configure source directories
	cfg.AddSourceDirectory("/home/user/documents", "docs")
	cfg.AddSourceDirectory("/home/user/pictures", "pics")
	cfg.AddSourceDirectory("/home/user/videos", "vids")

	// Configure restore destinations
	cfg.SetRestoreDestination("docs", "/mnt/restore/documents")
	cfg.SetRestoreDestination("pics", "") // Use source path

	// Save
	if err := cfg.WriteXmlCfg(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Load in new instance (simulate restart)
	cfg2, _ := NewMdConfig(configPath)

	// Verify all data
	if !cfg2.HasLabel("backup-vol-1") || !cfg2.HasLabel("backup-vol-2") {
		t.Error("Volume labels lost")
	}

	paths := cfg2.GetSourcePaths()
	if len(paths) != 3 {
		t.Errorf("Expected 3 source paths, got %d", len(paths))
	}

	// Verify alias lookup
	if alias := cfg2.GetAliasForPath("/home/user/documents"); alias != "docs" {
		t.Errorf("Alias lookup failed: got %q, want 'docs'", alias)
	}

	// Modify - remove one source
	cfg2.RemoveSourceDirectory("vids")

	// Add new volume
	cfg2.AddLabel("backup-vol-3")

	// Save again
	cfg2.WriteXmlCfg()

	// Load again
	cfg3, _ := NewMdConfig(configPath)

	if cfg3.HasSourceDirectory("vids") {
		t.Error("Removed source directory still present")
	}
	if !cfg3.HasLabel("backup-vol-3") {
		t.Error("New label not saved")
	}
	if len(cfg3.GetSourcePaths()) != 2 {
		t.Errorf("Expected 2 source paths after removal, got %d", len(cfg3.GetSourcePaths()))
	}
}
