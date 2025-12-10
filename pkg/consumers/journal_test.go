package consumers

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNewJournal tests basic journal creation and population
func TestNewJournal(t *testing.T) {
	journal, err := NewJournal()
	if err != nil {
		t.Fatalf("NewJournal failed: %v", err)
	}
	if journal == nil {
		t.Error("NewJournal should create a journal")
	}
	defer journal.Cleanup()

	if len(journal.String()) == 0 {
		t.Log("Empty journal is empty string - expected")
	}
}

// TestJournalXMLRoundTrip tests writing and reading journal XML
func TestJournalXMLRoundTrip(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "journal-xml-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create test files
	testDir := filepath.Join(tempDir, "test")
	if err := os.Mkdir(testDir, 0o755); err != nil {
		t.Fatalf("Failed to create test dir: %v", err)
	}

	if err := os.WriteFile(filepath.Join(testDir, "file1.txt"), []byte("content1"), 0o644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	// Run CheckCalc
	checkCalcOpts := CheckCalcOptions{
		CalcCount: 1,
		Recalc:    false,
		Validate:  false,
		Scrub:     false,
		AutoFix:   nil,
	}
	if err := RunCheckCalc([]string{testDir}, checkCalcOpts); err != nil {
		t.Fatalf("Failed to run CheckCalc: %v", err)
	}

	// Create and populate journal
	journal, err := NewJournal()
	if err != nil {
		t.Fatalf("NewJournal failed: %v", err)
	}
	defer journal.Cleanup()

	if err := journal.PopulateFromDirectories(testDir, "test-alias"); err != nil {
		t.Fatalf("Failed to populate journal: %v", err)
	}

	// Close the journal to ensure files are synced before reading
	if err := journal.Close(); err != nil {
		t.Fatalf("Failed to close journal: %v", err)
	}

	// Write to buffer
	var buf bytes.Buffer
	if err := journal.ToWriter(&buf); err != nil {
		t.Fatalf("Failed to write journal: %v", err)
	}

	if !bytes.Contains(buf.Bytes(), []byte(`<mdj alias="test-alias"`)) {
		t.Error("Journal XML should contain expected alias")
	}
	if !bytes.Contains(buf.Bytes(), []byte("</mdj>")) {
		t.Error("Journal XML should have closing mdj tag")
	}

	// Read back
	journalRead, err := NewJournal()
	if err != nil {
		t.Fatalf("NewJournal failed: %v", err)
	}
	defer journalRead.Cleanup()

	if err := journalRead.FromReader(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("Failed to read journal: %v", err)
	}

	// Close to sync files
	if err := journalRead.Close(); err != nil {
		t.Fatalf("Failed to close journal: %v", err)
	}

	// Verify both journals have content
	if len(journal.String()) == 0 {
		t.Error("Original journal should not be empty")
	}
	if len(journalRead.String()) == 0 {
		t.Error("Read journal should not be empty")
	}
}

// TestJournalFromReaderParsesAlias verifies FromReader correctly extracts alias
func TestJournalFromReaderParsesAlias(t *testing.T) {
	xmlData := `<mdj alias="photos">
  <dr dir="/path/to/photos">
    <fr fname="pic.jpg" checksum="abc123" mtime="1234567890" size="1024"></fr>
  </dr>
</mdj>`

	journal, err := NewJournal()
	if err != nil {
		t.Fatalf("NewJournal failed: %v", err)
	}
	defer journal.Cleanup()

	if err := journal.FromReader(bytes.NewReader([]byte(xmlData))); err != nil {
		t.Fatalf("Failed to read journal: %v", err)
	}

	// Close to sync files
	if err := journal.Close(); err != nil {
		t.Fatalf("Failed to close journal: %v", err)
	}

	journalStr := journal.String()
	if journalStr == "" {
		t.Error("Journal should have entries after reading XML")
	}
}

// TestJournalAliasRequiredInPopulate tests alias validation
func TestJournalAliasRequiredInPopulate(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "journal-alias-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	if err := os.WriteFile(filepath.Join(tempDir, "test.txt"), []byte("content"), 0o644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	// Run CheckCalc
	checkCalcOpts := CheckCalcOptions{
		CalcCount: 1,
		Recalc:    false,
		Validate:  false,
		Scrub:     false,
		AutoFix:   nil,
	}
	if err := RunCheckCalc([]string{tempDir}, checkCalcOpts); err != nil {
		t.Fatalf("Failed to run CheckCalc: %v", err)
	}

	journal, err := NewJournal()
	if err != nil {
		t.Fatalf("NewJournal failed: %v", err)
	}
	defer journal.Cleanup()

	// Should fail with empty alias
	if err := journal.PopulateFromDirectories(tempDir, ""); err != ErrAliasRequired {
		t.Errorf("Expected ErrAliasRequired for empty alias, got %v", err)
	}

	// Should succeed with valid alias
	if err := journal.PopulateFromDirectories(tempDir, "valid-alias"); err != nil {
		t.Errorf("Should succeed with valid alias, got %v", err)
	}
}

// TestJournalAliasRequiredInFromReader tests alias validation on read
func TestJournalAliasRequiredInFromReader(t *testing.T) {
	xmlData := `<mdj alias="">
  <dr dir="/path/to/dir">
    <fr fname="file.txt" checksum="xyz" mtime="1234567890" size="512"></fr>
  </dr>
</mdj>`

	journal, err := NewJournal()
	if err != nil {
		t.Fatalf("NewJournal failed: %v", err)
	}
	defer journal.Cleanup()

	if err := journal.FromReader(bytes.NewReader([]byte(xmlData))); err != ErrAliasRequired {
		t.Errorf("Expected ErrAliasRequired for empty alias in XML, got %v", err)
	}
}

// TestJournalAddIgnorePattern verifies that ignore patterns skip matching paths
func TestJournalAddIgnorePattern(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "journal-ignore-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create files: one to keep, one under "Recycle Bin" to ignore
	keepFile := filepath.Join(tempDir, "keep.txt")
	ignoreDir := filepath.Join(tempDir, "Recycle Bin", "bob")
	ignoreFile := filepath.Join(ignoreDir, "my.txt")

	if err := os.MkdirAll(ignoreDir, 0o755); err != nil {
		t.Fatalf("Failed to create ignore dir: %v", err)
	}
	if err := os.WriteFile(keepFile, []byte("keep"), 0o644); err != nil {
		t.Fatalf("Failed to write keep file: %v", err)
	}
	if err := os.WriteFile(ignoreFile, []byte("ignore"), 0o644); err != nil {
		t.Fatalf("Failed to write ignore file: %v", err)
	}

	// Generate .medorg.xml metadata
	checkCalcOpts := CheckCalcOptions{CalcCount: 1, Recalc: false, Validate: false, Scrub: false, AutoFix: nil}
	if err := RunCheckCalc([]string{tempDir}, checkCalcOpts); err != nil {
		t.Fatalf("Failed to run CheckCalc: %v", err)
	}

	journal, err := NewJournal()
	if err != nil {
		t.Fatalf("NewJournal failed: %v", err)
	}
	defer journal.Cleanup()

	if err := journal.AddIgnorePattern("Recycle Bin"); err != nil {
		t.Fatalf("Failed to add ignore pattern: %v", err)
	}

	if err := journal.PopulateFromDirectories(tempDir, "alias"); err != nil {
		t.Fatalf("Failed to populate journal: %v", err)
	}

	var buf bytes.Buffer
	if err := journal.ToWriter(&buf); err != nil {
		t.Fatalf("Failed to write journal: %v", err)
	}

	xmlOut := buf.String()
	if strings.Contains(xmlOut, "Recycle Bin") {
		t.Errorf("Expected files under 'Recycle Bin' to be ignored, but found entry: %s", xmlOut)
	}
	if !strings.Contains(xmlOut, "keep.txt") {
		t.Errorf("Expected keep.txt to be present in journal output")
	}
}
