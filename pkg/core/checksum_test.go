package core

import (
	"encoding/xml"
	"log"
	"os"
	"testing"
)

func TestB2B(t *testing.T) {
	// Back to Back check
	// uses current directory as an example
	// Just reads in the directory, creates an XML
	// Representation then reads in that XML representation
	// Then prints that out as well for good measure

	dirToProc := "."

	log.Println("Processing Directory", dirToProc)
	files, err := os.ReadDir(dirToProc)
	if err != nil {
		t.Error(err)
	}

	var bob Md5File

	for _, file := range files {
		bob.Files = append(bob.Files, FileStruct{Name: Fname(file.Name())})
	}

	marshelled, err := xml.MarshalIndent(bob, "", "  ")
	if err != nil {
		t.Fatal("marshall error", err)
	}
	var fred Md5File
	err = xml.Unmarshal([]byte(marshelled), &fred)
	if err != nil {
		t.Fatal("um error", err)
	}
}
