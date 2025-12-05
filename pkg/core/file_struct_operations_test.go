package core

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestTagManagement tests AddTag and RemoveTag operations
func TestTagManagement(t *testing.T) {
	tests := []struct {
		name  string
		tags  []string
		add   string
		want  bool // expected return value from AddTag
		check bool // check if tag is present after add
	}{
		{"add to empty", []string{}, "tag1", true, true},
		{"add duplicate", []string{"tag1"}, "tag1", false, true},
		{"add to existing", []string{"tag1", "tag2"}, "tag3", true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := FileStruct{BackupDest: tt.tags}
			got := fs.AddTag(tt.add)

			if got != tt.want {
				t.Errorf("AddTag returned %v, want %v", got, tt.want)
			}

			if tt.check {
				if !fs.HasTag(tt.add) {
					t.Errorf("Tag %q not found after adding", tt.add)
				}
			}
		})
	}
}

// TestRemoveTag tests removal of tags
func TestRemoveTag(t *testing.T) {
	tests := []struct {
		name     string
		tags     []string
		remove   string
		want     bool // expected return from RemoveTag
		wantLeft []string
	}{
		{"remove from single", []string{"tag1"}, "tag1", true, []string{}},
		{"remove from multiple", []string{"tag1", "tag2", "tag3"}, "tag2", true, []string{"tag1", "tag3"}},
		{"remove nonexistent", []string{"tag1"}, "tag2", false, []string{"tag1"}},
		{"remove from empty", []string{}, "tag1", false, []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := FileStruct{BackupDest: tt.tags}
			got := fs.RemoveTag(tt.remove)

			if got != tt.want {
				t.Errorf("RemoveTag returned %v, want %v", got, tt.want)
			}

			if len(fs.BackupDest) != len(tt.wantLeft) {
				t.Errorf("After remove, got %d tags, want %d", len(fs.BackupDest), len(tt.wantLeft))
			}

			for _, tag := range tt.wantLeft {
				if !fs.HasTag(tag) {
					t.Errorf("Expected tag %q to remain", tag)
				}
			}
		})
	}
}

// TestHasTag tests tag detection
func TestHasTag(t *testing.T) {
	fs := FileStruct{BackupDest: []string{"backup1", "backup2", "backup3"}}

	tests := []struct {
		tag  string
		want bool
	}{
		{"backup1", true},
		{"backup2", true},
		{"backup3", true},
		{"backup4", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.tag, func(t *testing.T) {
			got := fs.HasTag(tt.tag)
			if got != tt.want {
				t.Errorf("HasTag(%q) = %v, want %v", tt.tag, got, tt.want)
			}
		})
	}
}

// TestBackupDestinations tests backup destination accessor
func TestBackupDestinations(t *testing.T) {
	dests := []string{"vol1", "vol2", "vol3"}
	fs := FileStruct{BackupDest: dests}

	result := fs.BackupDestinations()

	if len(result) != len(dests) {
		t.Errorf("Got %d destinations, want %d", len(result), len(dests))
	}

	for i, d := range dests {
		if result[i] != d {
			t.Errorf("Destination %d: got %q, want %q", i, result[i], d)
		}
	}
}

// TestAddBackupDestination tests backup destination addition
func TestAddBackupDestination(t *testing.T) {
	fs := FileStruct{}

	fs.AddBackupDestination("vol1")
	if !fs.HasBackupOn("vol1") {
		t.Error("Expected file to have backup on vol1")
	}

	fs.AddBackupDestination("vol2")
	if !fs.HasBackupOn("vol2") {
		t.Error("Expected file to have backup on vol2")
	}

	// Adding same destination twice should be idempotent with tags
	fs.AddBackupDestination("vol1")
	count := len(fs.BackupDest)
	if count != 2 {
		t.Errorf("Expected 2 destinations after adding vol1 twice, got %d", count)
	}
}

// TestHasBackupOn tests backup location checking
func TestHasBackupOn(t *testing.T) {
	fs := FileStruct{BackupDest: []string{"backup_a", "backup_b"}}

	tests := []struct {
		label string
		want  bool
	}{
		{"backup_a", true},
		{"backup_b", true},
		{"backup_c", false},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			got := fs.HasBackupOn(tt.label)
			if got != tt.want {
				t.Errorf("HasBackupOn(%q) = %v, want %v", tt.label, got, tt.want)
			}
		})
	}
}

// TestSetDirectory tests directory setter
func TestSetDirectory(t *testing.T) {
	fs := FileStruct{directory: "old"}
	fs.SetDirectory("new")

	if fs.Directory() != "new" {
		t.Errorf("Directory after set: got %q, want 'new'", fs.Directory())
	}
}

// TestFileStructDirectory tests directory getter
func TestFileStructDirectory(t *testing.T) {
	fs := FileStruct{directory: "/path/to/dir"}
	result := fs.Directory()

	if result != "/path/to/dir" {
		t.Errorf("Directory: got %q, want '/path/to/dir'", result)
	}
}

// TestFileStructPath tests path generation
func TestFileStructPath(t *testing.T) {
	fs := FileStruct{directory: "/home/user/docs", Name: "file.txt"}
	result := fs.Path()

	expected := NewFpath("/home/user/docs", "file.txt")
	if result.String() != expected.String() {
		t.Errorf("Path: got %v, want %v", result, expected)
	}
}

// TestUpdateChecksumWithoutForce tests checksum calculation without force
func TestUpdateChecksumWithoutForce(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	// Create test file
	if err := os.WriteFile(testFile, []byte("test content"), 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	fs := FileStruct{directory: tmpDir, Name: "test.txt", Checksum: "existing"}

	// Without force, should skip if checksum exists
	err := fs.UpdateChecksum(false)
	if err != nil {
		t.Errorf("UpdateChecksum returned error: %v", err)
	}
	if fs.Checksum != "existing" {
		t.Error("UpdateChecksum should not change existing checksum without force")
	}
}

// TestUpdateChecksumWithForce tests checksum calculation with force
func TestUpdateChecksumWithForce(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	// Create test file
	if err := os.WriteFile(testFile, []byte("test content"), 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	fs := FileStruct{directory: tmpDir, Name: "test.txt", Checksum: "old"}

	// With force, should recalculate
	err := fs.UpdateChecksum(true)
	if err != nil {
		t.Errorf("UpdateChecksum returned error: %v", err)
	}
	if fs.Checksum == "old" || fs.Checksum == "" {
		t.Error("UpdateChecksum should calculate new checksum with force=true")
	}
}

// TestUpdateChecksumClearsBackupDest tests that changed checksum clears backups
func TestUpdateChecksumClearsBackupDest(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	// Create test file
	if err := os.WriteFile(testFile, []byte("test content"), 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	fs := FileStruct{
		directory:  tmpDir,
		Name:       "test.txt",
		Checksum:   "old_checksum",
		BackupDest: []string{"backup1", "backup2"},
	}

	// Update with force should clear backups
	_ = fs.UpdateChecksum(true)

	if len(fs.BackupDest) != 0 {
		t.Errorf("Expected BackupDest to be cleared, got %v", fs.BackupDest)
	}
}

// TestValidateChecksumMatch tests checksum validation when valid
func TestValidateChecksumMatch(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")
	content := []byte("test content")

	// Create test file
	if err := os.WriteFile(testFile, content, 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Calculate correct checksum
	fs := FileStruct{directory: tmpDir, Name: "test.txt", Checksum: ""}
	_ = fs.UpdateChecksum(true)
	correctChecksum := fs.Checksum

	// Create new struct with correct checksum
	fs2 := FileStruct{
		directory:  tmpDir,
		Name:       "test.txt",
		Checksum:   correctChecksum,
		BackupDest: []string{"backup1"},
	}

	err := fs2.ValidateChecksum()
	if err != nil {
		t.Errorf("ValidateChecksum returned error for valid checksum: %v", err)
	}
	if fs2.Checksum != correctChecksum {
		t.Error("ValidateChecksum should not change matching checksum")
	}
	if len(fs2.BackupDest) != 1 {
		t.Error("ValidateChecksum should not clear BackupDest for matching checksum")
	}
}

// TestValidateChecksumMismatch tests checksum validation when invalid
func TestValidateChecksumMismatch(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	// Create test file
	if err := os.WriteFile(testFile, []byte("test content"), 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	fs := FileStruct{
		directory:  tmpDir,
		Name:       "test.txt",
		Checksum:   "wrong_checksum_value",
		BackupDest: []string{"backup1", "backup2"},
	}

	err := fs.ValidateChecksum()
	if err != ErrRecalced {
		t.Errorf("ValidateChecksum should return ErrRecalced for mismatch, got %v", err)
	}
	if len(fs.BackupDest) != 0 {
		t.Errorf("ValidateChecksum should clear BackupDest on mismatch, got %v", fs.BackupDest)
	}
}

// TestFileStructEqual tests equality comparison
func TestFileStructEqual(t *testing.T) {
	tests := []struct {
		name string
		fs1  FileStruct
		fs2  FileStruct
		want bool
	}{
		{
			"same checksum and size",
			FileStruct{Checksum: "abc123", Size: 100},
			FileStruct{Checksum: "abc123", Size: 100},
			true,
		},
		{
			"different checksum",
			FileStruct{Checksum: "abc123", Size: 100},
			FileStruct{Checksum: "def456", Size: 100},
			false,
		},
		{
			"different size",
			FileStruct{Checksum: "abc123", Size: 100},
			FileStruct{Checksum: "abc123", Size: 200},
			false,
		},
		{
			"empty checksum fs1",
			FileStruct{Checksum: "", Size: 100},
			FileStruct{Checksum: "abc123", Size: 100},
			false,
		},
		{
			"empty checksum fs2",
			FileStruct{Checksum: "abc123", Size: 100},
			FileStruct{Checksum: "", Size: 100},
			false,
		},
		{
			"both empty checksums",
			FileStruct{Checksum: "", Size: 100},
			FileStruct{Checksum: "", Size: 100},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.fs1.Equal(tt.fs2)
			if got != tt.want {
				t.Errorf("Equal returned %v, want %v", got, tt.want)
			}
		})
	}
}

// TestGetChecksum tests checksum getter
func TestGetChecksum(t *testing.T) {
	fs := FileStruct{Checksum: "test_checksum_value"}
	result := fs.GetChecksum()

	if result != "test_checksum_value" {
		t.Errorf("GetChecksum returned %q, want 'test_checksum_value'", result)
	}
}

// TestGetSize tests size getter
func TestGetSize(t *testing.T) {
	fs := FileStruct{Size: 12345}
	result := fs.GetSize()

	if result != 12345 {
		t.Errorf("GetSize returned %d, want 12345", result)
	}
}

// TestGetName tests name getter
func TestGetName(t *testing.T) {
	fs := FileStruct{Name: "myfile.txt"}
	result := fs.GetName()

	if result != "myfile.txt" {
		t.Errorf("GetName returned %q, want 'myfile.txt'", result)
	}
}

// TestGetTags tests tags getter
func TestGetTags(t *testing.T) {
	originalTags := []string{"tag1", "tag2", "tag3"}
	fs := FileStruct{Tags: originalTags}

	result := fs.GetTags()

	if len(result) != len(originalTags) {
		t.Errorf("GetTags returned %d tags, want %d", len(result), len(originalTags))
	}

	for i, tag := range originalTags {
		if result[i] != tag {
			t.Errorf("Tag %d: got %q, want %q", i, result[i], tag)
		}
	}

	// Verify it's a copy, not a reference
	result[0] = "modified"
	if fs.Tags[0] == "modified" {
		t.Error("GetTags returned reference instead of copy")
	}
}

// TestIndexTag tests internal tag indexing
func TestIndexTag(t *testing.T) {
	fs := FileStruct{BackupDest: []string{"dest1", "dest2", "dest3"}}

	tests := []struct {
		tag  string
		want int
	}{
		{"dest1", 0},
		{"dest2", 1},
		{"dest3", 2},
		{"notfound", -1},
	}

	for _, tt := range tests {
		t.Run(tt.tag, func(t *testing.T) {
			got := fs.indexTag(tt.tag)
			if got != tt.want {
				t.Errorf("indexTag(%q) = %d, want %d", tt.tag, got, tt.want)
			}
		})
	}
}

// TestNewFileStruct tests file struct creation from file
func TestNewFileStruct(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	// Create test file
	testContent := []byte("test content for file struct")
	if err := os.WriteFile(testFile, testContent, 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Get file stat for reference time
	stat, _ := os.Stat(testFile)
	expectedMtime := stat.ModTime().Unix()
	expectedSize := stat.Size()

	fs, err := NewFileStruct(tmpDir, "test.txt")
	if err != nil {
		t.Fatalf("NewFileStruct returned error: %v", err)
	}

	if fs.Name != "test.txt" {
		t.Errorf("Name: got %q, want 'test.txt'", fs.Name)
	}

	if fs.Mtime != expectedMtime {
		t.Errorf("Mtime: got %d, want %d", fs.Mtime, expectedMtime)
	}

	if fs.Size != expectedSize {
		t.Errorf("Size: got %d, want %d", fs.Size, expectedSize)
	}

	if fs.Directory() != tmpDir {
		t.Errorf("Directory: got %q, want %q", fs.Directory(), tmpDir)
	}

	if fs.Checksum != "" {
		t.Error("Checksum should be empty for newly created FileStruct")
	}
}

// TestFromStat tests FileStruct creation from FileInfo
func TestFromStat(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	// Create test file
	testContent := []byte("stat test content")
	if err := os.WriteFile(testFile, testContent, 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	stat, _ := os.Stat(testFile)
	fs := FileStruct{}

	result, err := fs.FromStat(tmpDir, "test.txt", stat)
	if err != nil {
		t.Fatalf("FromStat returned error: %v", err)
	}

	if result.Name != "test.txt" {
		t.Errorf("Name: got %q, want 'test.txt'", result.Name)
	}

	if result.Size != stat.Size() {
		t.Errorf("Size: got %d, want %d", result.Size, stat.Size())
	}

	if result.Directory() != tmpDir {
		t.Errorf("Directory: got %q, want %q", result.Directory(), tmpDir)
	}
}

// TestChanged tests change detection
func TestChanged(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	// Create initial file
	if err := os.WriteFile(testFile, []byte("initial"), 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Get initial stat
	stat1, _ := os.Stat(testFile)

	// Create FileStruct from initial state
	fs := FileStruct{
		Mtime: stat1.ModTime().Unix(),
		Size:  stat1.Size(),
	}

	// Should not have changed
	changed, err := fs.Changed(stat1)
	if err != nil {
		t.Fatalf("Changed returned error: %v", err)
	}
	if changed {
		t.Error("Changed should return false for unchanged file")
	}

	// Wait a bit and modify file
	time.Sleep(10 * time.Millisecond)
	if err := os.WriteFile(testFile, []byte("modified content"), 0o644); err != nil {
		t.Fatalf("Failed to modify test file: %v", err)
	}

	// Get new stat
	stat2, _ := os.Stat(testFile)

	// Should have changed
	changed, err = fs.Changed(stat2)
	if err != nil {
		t.Fatalf("Changed returned error: %v", err)
	}
	if !changed {
		t.Error("Changed should return true for modified file")
	}
}

// TestConflictingTagOperations tests tag operations under conflict conditions
func TestConflictingTagOperations(t *testing.T) {
	fs := FileStruct{BackupDest: []string{"tag1", "tag2"}}

	// Try to add a tag that's already there
	result := fs.AddTag("tag1")
	if result {
		t.Error("AddTag should return false for existing tag")
	}

	// Now add a new one
	result = fs.AddTag("tag3")
	if !result {
		t.Error("AddTag should return true for new tag")
	}

	// Remove and re-add
	fs.RemoveTag("tag3")
	result = fs.AddTag("tag3")
	if !result {
		t.Error("AddTag should return true after removal")
	}
}
