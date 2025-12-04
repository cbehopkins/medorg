package core

import (
	"crypto/rand"
	"encoding/xml"
	"log"
	"os"
	"os/exec"
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
		bob.Files = append(bob.Files, FileStruct{Name: file.Name()})
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

func makeFile(directory string) string {
	buff := make([]byte, 75000)
	if _, err := rand.Read(buff); err != nil {
		panic(err)
	}
	tmpfile, err := os.CreateTemp(directory, "example")
	if err != nil {
		panic(err)
	}
	if _, err := tmpfile.Write(buff); err != nil {
		panic(err)
	}
	if err := tmpfile.Close(); err != nil {
		panic(err)
	}
	return tmpfile.Name()
}

func TestMd5(t *testing.T) {
	// Check the MD5 creation mechanism

	tmp_filename := makeFile(".")
	defer os.Remove(tmp_filename) // clean up

	// So get us a channel to send the files to be md5'd to
	// This returns 2 channels, one that files to be checked should be sent to
	// a second that gives the filename and checksum (sent as a file object)
	toMd5Chan, toUpdateXML, closedChan := NewChannels()

	// Now get the json manager
	// We only have one of these to make file locks easier
	// This receives a file structure on one channel, opens that file and modifies it accordingly.
	// In the final application this will be the only thing that can update the xml files
	wg, errChan := newXMLManager(toUpdateXML)

	toMd5Chan <- FileStruct{Name: tmp_filename, directory: "."}
	log.Println("Sent the file to check")
	close(toMd5Chan)
	log.Println("Waiting for channel to close")
	<-closedChan
	log.Println("Channel Closed, waiting for XmlManager to complete")
	wg.Wait()
	// Drain any remaining errors
	for err := range errChan {
		if err != nil {
			t.Error("XML manager error:", err)
		}
	}
	log.Println("All done")
}

func TestSelfCompat(t *testing.T) {
	fileToUse := "checksum_test.go"
	_ = md5FileWrite(".", nil)

	toMd5Chan, toUpdateXML, closedChan := NewChannels()
	wg, errChan := newXMLManager(toUpdateXML)
	toMd5Chan <- FileStruct{Name: fileToUse, directory: "."}
	close(toMd5Chan)
	<-closedChan
	wg.Wait()
	// Drain any remaining errors
	for err := range errChan {
		if err != nil {
			t.Error("XML manager error:", err)
		}
	}
	dm, err := DirectoryMapFromDir(".")
	if err != nil {
		t.Error(err)
	}
	v, ok := dm.Get(fileToUse)
	if !ok {
		t.Fatal(fileToUse, " is gone!!!", dm)
	}
	newChecksum := v.Checksum
	if newChecksum == "" {
		t.Fatal("Missing Checksum from go version")
	}
}

func TestPerlCompat(t *testing.T) {
	perlScript := "/home/cbh/home/script/perl/file_check.pl"
	if _, err := os.Stat(perlScript); os.IsNotExist(err) {
		t.Skip()
	}

	fileToUse := "checksum_test.go"
	if _, err := os.Stat("./" + Md5FileName); os.IsExist(err) {
		_ = os.Remove("./" + Md5FileName)
	}
	log.Println("Running Command", perlScript)
	cmd := exec.Command(perlScript, ".")
	err := cmd.Run()
	if err != nil {
		t.Error(err)
	}
	log.Println("Command Run")

	dm, err := DirectoryMapFromDir(".")
	if err != nil {
		t.Error(err)
	}
	v, ok := dm.Get(fileToUse)
	if !ok {
		t.Error(fileToUse, " is gone!", dm)
	}
	checksum := v.Checksum
	if checksum == "" {
		t.Error("Missing Checksum from perl version")
	}
	_ = os.Remove("./" + Md5FileName)
	toMd5Chan, toUpdateXML, closedChan := NewChannels()
	wg, errChan := newXMLManager(toUpdateXML)
	toMd5Chan <- FileStruct{Name: fileToUse, directory: "."}
	close(toMd5Chan)
	<-closedChan
	wg.Wait()
	// Drain any remaining errors
	for err := range errChan {
		if err != nil {
			t.Error("XML manager error:", err)
		}
	}
	dm, err = DirectoryMapFromDir(".")
	if err != nil {
		t.Error(err)
	}
	v, ok = dm.Get(fileToUse)
	if !ok {
		t.Error(fileToUse, "Bob is gone for a second time!")
	}
	newChecksum := v.Checksum
	if newChecksum == "" {
		t.Error("Missing Checksum from go version")
	}
	if newChecksum != checksum {
		t.Error("checksums don't tally. New:", newChecksum, " Old:", checksum)
	}
}
