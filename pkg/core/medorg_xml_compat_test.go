package core

import (
	"strings"
	"testing"
)

func TestMedorgXMLCompatibilityFixtures(t *testing.T) {
	tests := []struct {
		name          string
		xml           string
		wantDir       string
		wantFileCount int
		check         func(t *testing.T, dm DirectoryMap)
	}{
		{
			name: "minimal file entry",
			xml: `<dr>
  <fr fname="legacy.txt" checksum="abc123" size="12"></fr>
</dr>`,
			wantDir:       "",
			wantFileCount: 1,
			check: func(t *testing.T, dm DirectoryMap) {
				fs, ok := dm.Get("legacy.txt")
				if !ok {
					t.Fatalf("expected legacy.txt to be present")
				}
				if fs.Checksum != "abc123" || fs.Size != 12 {
					t.Fatalf("unexpected file values: %+v", fs)
				}
			},
		},
		{
			name: "dir attribute with tags and backup destinations",
			xml: `<dr dir="old/path">
  <fr fname="photo.jpg" checksum="base64hash" mtime="1710000000" size="1024">
    <tag>favorite</tag>
    <tag>family</tag>
    <bd>VOL1</bd>
    <bd>VOL2</bd>
  </fr>
</dr>`,
			wantDir:       "old/path",
			wantFileCount: 1,
			check: func(t *testing.T, dm DirectoryMap) {
				fs, ok := dm.Get("photo.jpg")
				if !ok {
					t.Fatalf("expected photo.jpg to be present")
				}
				if fs.Mtime != 1710000000 || fs.Size != 1024 {
					t.Fatalf("unexpected mtime/size: %+v", fs)
				}
				if len(fs.Tags) != 2 || fs.Tags[0] != "favorite" || fs.Tags[1] != "family" {
					t.Fatalf("unexpected tags: %+v", fs.Tags)
				}
				if len(fs.BackupDest) != 2 || fs.BackupDest[0] != "VOL1" || fs.BackupDest[1] != "VOL2" {
					t.Fatalf("unexpected backup destinations: %+v", fs.BackupDest)
				}
			},
		},
		{
			name: "unknown attributes are ignored",
			xml: `<dr unknown="field">
  <fr fname="misc.bin" checksum="hash2" size="99" extraneous="true"></fr>
</dr>`,
			wantDir:       "",
			wantFileCount: 1,
			check: func(t *testing.T, dm DirectoryMap) {
				if _, ok := dm.Get("misc.bin"); !ok {
					t.Fatalf("expected misc.bin to be present")
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dm := *newDirectoryMap()
			dir, err := dm.FromXML([]byte(tc.xml))
			if err != nil {
				t.Fatalf("FromXML failed: %v", err)
			}
			if string(dir) != tc.wantDir {
				t.Fatalf("dir mismatch: got %q want %q", string(dir), tc.wantDir)
			}
			if dm.Len() != tc.wantFileCount {
				t.Fatalf("file count mismatch: got %d want %d", dm.Len(), tc.wantFileCount)
			}
			tc.check(t, dm)
		})
	}
}

func TestMedorgXMLGoldenSerializationSingleFile(t *testing.T) {
	dm := *newDirectoryMap()
	dm.Add(FileStruct{
		Name:       "golden.txt",
		Checksum:   "A1B2C3",
		Mtime:      1700000000,
		Size:       42,
		Tags:       []string{"important", "verified"},
		BackupDest: []string{"VOL_A", "VOL_B"},
	})

	got, err := dm.ToXML("ignored")
	if err != nil {
		t.Fatalf("ToXML failed: %v", err)
	}

	want := `<dr>
  <fr fname="golden.txt" checksum="A1B2C3" mtime="1700000000" size="42">
    <tag>important</tag>
    <tag>verified</tag>
    <bd>VOL_A</bd>
    <bd>VOL_B</bd>
  </fr>
</dr>`

	if strings.TrimSpace(string(got)) != strings.TrimSpace(want) {
		t.Fatalf("golden serialization mismatch\nGot:\n%s\nWant:\n%s", string(got), want)
	}
}
