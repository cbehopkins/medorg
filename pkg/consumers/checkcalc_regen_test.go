package consumers

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

// TestRunCheckCalcRegeneratesCorruptedMedorgXml ensures that when the directory
// metadata file (.medorg.xml) is corrupted (e.g., wrong root element), running
// RunCheckCalc will tolerate the unmarshal error, rebuild entries from disk, and
// persist a valid .medorg.xml containing file checksums.
func TestRunCheckCalcRegeneratesCorruptedMedorgXml(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	// Create a real file that should be discovered and checksummed
	dataPath := filepath.Join(dir, "a.txt")
	if err := os.WriteFile(dataPath, []byte("hello world"), 0o644); err != nil {
		t.Fatalf("failed to write data file: %v", err)
	}

	// Seed a corrupted .medorg.xml with the wrong root element to trigger
	// an xml.UnmarshalError similar to: expected element type <dr> but have <xc>
	badXML := []byte(`<?xml version="1.0"?><xc></xc>`) // wrong root tag
	mdPath := filepath.Join(dir, core.Md5FileName)
	if err := os.WriteFile(mdPath, badXML, 0o644); err != nil {
		t.Fatalf("failed to write corrupted %s: %v", core.Md5FileName, err)
	}

	// Run mdcalc core to rebuild/refresh checksums
	opts := CheckCalcOptions{CalcCount: 1}
	if err := RunCheckCalc([]string{dir}, opts); err != nil {
		t.Fatalf("RunCheckCalc failed: %v", err)
	}

	// Now the .medorg.xml should be regenerated and valid
	// Load it via DirectoryMapFromDir and validate entries
	dm, err := core.DirectoryMapFromDir(core.Dirname(dir))
	if err != nil {
		t.Fatalf("DirectoryMapFromDir returned error after regeneration: %v", err)
	}

	if dm.Len() != 1 {
		t.Fatalf("expected 1 file entry after regeneration, got %d", dm.Len())
	}

	// Ensure checksum exists for the known file
	fs, ok := dm.Get(core.Fname("a.txt"))
	if !ok {
		t.Fatalf("expected entry for a.txt not found in regenerated metadata")
	}
	if fs.Checksum == "" {
		t.Fatalf("expected checksum to be populated for a.txt")
	}
}
