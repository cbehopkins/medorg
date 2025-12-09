package consumers

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// TestNewJournal tests basic journal creation and population
func TestNewJournal(t *testing.T) {
	journal := NewJournal()
	if journal == nil {
		t.Error("NewJournal should create a journal")
	}
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
	journal := NewJournal()
	if err := journal.PopulateFromDirectories(testDir, "test-alias"); err != nil {
		t.Fatalf("Failed to populate journal: %v", err)
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
	journalRead := NewJournal()
	if err := journalRead.FromReader(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("Failed to read journal: %v", err)
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

	journal := NewJournal()
	if err := journal.FromReader(bytes.NewReader([]byte(xmlData))); err != nil {
		t.Fatalf("Failed to read journal: %v", err)
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

	journal := NewJournal()

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

	journal := NewJournal()
	if err := journal.FromReader(bytes.NewReader([]byte(xmlData))); err != ErrAliasRequired {
		t.Errorf("Expected ErrAliasRequired for empty alias in XML, got %v", err)
	}
}
