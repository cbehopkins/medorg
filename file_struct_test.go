package medorg_test

import (
	"encoding/xml"
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/medorg"
)

func TestFileStructXMLRead(t *testing.T) {
    // Define the sample XML content
    xmlContent := `
<fr fname="example.txt" checksum="1234567890abcdef" mtime="1625097600" size="1024">
  <tag>tag1</tag>
  <tag>tag2</tag>
  <bd>some place</bd>
</fr>`

    // Create a temporary directory
    tempDir, err := os.MkdirTemp("", "filestruct_test")
    if err != nil {
        t.Fatalf("Failed to create temporary directory: %v", err)
    }
    defer os.RemoveAll(tempDir) // Clean up

    // Write the sample XML content to a file
    xmlFilePath := filepath.Join(tempDir, "filestruct.xml")
    if err := os.WriteFile(xmlFilePath, []byte(xmlContent), 0644); err != nil {
        t.Fatalf("Failed to write XML file: %v", err)
    }

    // Read the XML file
    xmlFile, err := os.Open(xmlFilePath)
    if err != nil {
        t.Fatalf("Failed to open XML file: %v", err)
    }
    defer xmlFile.Close()

    // Deserialize the XML content into a FileStruct
    var fs medorg.FileStruct
    if err := xml.NewDecoder(xmlFile).Decode(&fs); err != nil {
        t.Fatalf("Failed to decode XML content: %v", err)
    }

    // Define the expected FileStruct
    expected := medorg.FileStruct{
        Name:      "example.txt",
        Checksum:  "1234567890abcdef",
        Mtime:     1625097600,
        Size:      1024,
        Tags:      []string{"tag1", "tag2"},
        BackupDest: []string{"some place"},
    }

    // Compare the deserialized FileStruct with the expected values
    if fs.Name != expected.Name ||
        fs.Checksum != expected.Checksum ||
        fs.Mtime != expected.Mtime ||
        fs.Size != expected.Size ||
        len(fs.Tags) != len(expected.Tags) ||
        len(fs.BackupDest) != len(expected.BackupDest) {
        t.Fatalf("Deserialized FileStruct does not match expected values")
    }

    for i, tag := range fs.Tags {
        if tag != expected.Tags[i] {
            t.Fatalf("Deserialized FileStruct tags do not match expected values")
        }
    }

    for i, archivedAt := range fs.BackupDest {
        if archivedAt != expected.BackupDest[i] {
            t.Fatalf("Deserialized FileStruct Backup Dest does not match expected values")
        }
    }
}