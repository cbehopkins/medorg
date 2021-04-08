package medorg

import (
	"crypto/rand"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"testing"
)

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
func TestB2B(t *testing.T) {
	//Back to Back check
	// uses current directory as an example
	// Just reads in the directory, creates an XML
	// Representation then reads in that XML representation
	// Then prints that out as well for good measure

	dirToProc := "."

	log.Println("Processing Directory", dirToProc)
	files, err := ioutil.ReadDir(dirToProc)
	check(err)

	bob := NewMd5File()

	for _, file := range files {
		//log.Println(file.Name())
		bob.AddFile(file.Name())
	}

	log.Println(bob)
	marshelled := bob.String()
	fred := NewMd5File()
	err = fred.FromXML([]byte(marshelled))
	if err != nil {
		log.Fatal("um error", err)
	}
	log.Println(fred)

	log.Println("All Done")
}
func makeFile(directory string) string {
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
	wg := NewXMLManager(toUpdateXML)

	toMd5Chan <- FileStruct{Name: tmp_filename, directory: "."}
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
	removeMd5(".")

	dm := *NewDirectoryMap()
	toMd5Chan, toUpdateXML, closedChan := NewChannels()
	wg := NewXMLManager(toUpdateXML)
	toMd5Chan <- FileStruct{Name: fileToUse, directory: "."}
	close(toMd5Chan)
	<-closedChan
	wg.Wait()
	dm = DirectoryMapFromDir(".")
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
		log.Fatal("Erred!", err)
	}
	log.Println("Command Run")

	dm := DirectoryMapFromDir(".")
	v, ok := dm.Get(fileToUse)
	if !ok {
		log.Fatal(fileToUse, " is gone!", dm)
	}
	checksum := v.Checksum
	if checksum == "" {
		log.Fatal("Missing Checksum from perl version")
	}
	_ = os.Remove("./" + Md5FileName)
	dm = *NewDirectoryMap()
	toMd5Chan, toUpdateXML, closedChan := NewChannels()
	wg := NewXMLManager(toUpdateXML)
	toMd5Chan <- FileStruct{Name: fileToUse, directory: "."}
	close(toMd5Chan)
	<-closedChan
	wg.Wait()
	dm = DirectoryMapFromDir(".")
	v, ok = dm.Get(fileToUse)
	if !ok {
		log.Fatal(fileToUse, "Bob is gone for a second time!")

	}
	newChecksum := v.Checksum
	if newChecksum == "" {
		log.Fatal("Missing Checksum from go version")
	}
	if newChecksum != checksum {
		log.Fatal("checksums don't tally. New:", newChecksum, " Old:", checksum)
	}
}
