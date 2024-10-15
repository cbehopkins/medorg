package medorg

import (
	"crypto/rand"
	"encoding/xml"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"testing"
)

func TestB2B(t *testing.T) {
	//Back to Back check
	// uses current directory as an example
	// Just reads in the directory, creates an XML
	// Representation then reads in that XML representation
	// Then prints that out as well for good measure

	dirToProc := "."

	log.Println("Processing Directory", dirToProc)
	files, err := ioutil.ReadDir(dirToProc)
	if err != nil {
		t.Error(err)
	}

	var bob Md5File

	for _, file := range files {
		bob.Files = append(bob.Files, FileStruct{Name: file.Name()})
	}

	marshelled, err := xml.MarshalIndent(bob, "", "  ")
	if err != nil {
		log.Fatal("marshall error", err)
	}
	var fred Md5File
	err = xml.Unmarshal([]byte(marshelled), &fred)
	if err != nil {
		log.Fatal("um error", err)
	}
}
func makeFile(directory string) string {
	// FIXME it would be quicker to calculate the checksum here
	// while it's an in memory object
	buff := make([]byte, 75000)
	rand.Read(buff)
	tmpfile, err := ioutil.TempFile(directory, "example")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := tmpfile.Write(buff); err != nil {
		log.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}
	return tmpfile.Name()
}
func TestMd5(t *testing.T) {
	// Check the MD5 creation mechanism

	tmpFilename := makeFile(".")
	defer os.Remove(tmpFilename) // clean up

	// So get us a channel to send the files to be md5'd to
	// This returns 2 channels, one that files to be checked should be sent to
	// a second that gives the filename and checksum (sent as a file object)
	toMd5Chan, toUpdateXML, closedChan := NewChannels()

	// Now get the json manager
	// We only have one of these to make file locks easier
	// This receives a file structure on one channel, opens that file and modifies it accordingly.
	// In the final application this will be the only thing that can update the xml files
	wg := newXMLManager(toUpdateXML)

	toMd5Chan <- FileStruct{Name: tmpFilename, directory: "."}
	log.Println("Sent the file to check")
	close(toMd5Chan)
	log.Println("Waiting for channel to close")
	<-closedChan
	log.Println("Channel Closed, waiting for XmlManager to complete")
	wg.Wait()
	log.Println("All done")
}
func TestSelfCompat(t *testing.T) {
	fileToUse := "checksum_test.go"
	_ = md5FileWrite(".", nil)

	dm := *NewDirectoryMap(nil)
	toMd5Chan, toUpdateXML, closedChan := NewChannels()
	wg := newXMLManager(toUpdateXML)
	toMd5Chan <- FileStruct{Name: fileToUse, directory: "."}
	close(toMd5Chan)
	<-closedChan
	wg.Wait()
	dm, err := DirectoryMapFromDir(".", nil)
	if err != nil {
		t.Error(err)
	}
	v, ok := dm.Get(fileToUse)
	if !ok {
		log.Fatal(fileToUse, " is gone!!!", dm)
	}
	newChecksum := v.Checksum
	if newChecksum == "" {
		log.Fatal("Missing Checksum from go version")
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

	dm, err := DirectoryMapFromDir(".", nil)
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
	dm = *NewDirectoryMap(nil)
	toMd5Chan, toUpdateXML, closedChan := NewChannels()
	wg := newXMLManager(toUpdateXML)
	toMd5Chan <- FileStruct{Name: fileToUse, directory: "."}
	close(toMd5Chan)
	<-closedChan
	wg.Wait()
	dm, err = DirectoryMapFromDir(".", nil)
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
