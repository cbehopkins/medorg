package medorg

import (
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

	var dir_to_proc string
	dir_to_proc = "."
	//	if len(os.Args) > 1 {
	//		dir_to_proc = os.Args[1]
	//	}
	//	if dir_to_proc == "" {
	//		log.Fatalln("Please provide directory to process")
	//	}

	log.Println("Processing Directory", dir_to_proc)
	files, err := ioutil.ReadDir(dir_to_proc)
	check(err)

	bob := NewMd5File()

	for _, file := range files {
		//log.Println(file.Name())
		bob.AddFile(file.Name())
	}

	log.Println(bob)
	marshelled := bob.String()
	fred := NewMd5File()
	fred.UnmarshalXml([]byte(marshelled))
	log.Println(fred)

	log.Println("All Done")
}

func TestMd5(t *testing.T) {
	// Check the MD5 creation mechanism

	// So get us a channel to send the files to be md5'd to
	// This returns 2 channels, one that files to be checked should be sent to
	// a second that gives the filename and checksum (sent as a file object)
	to_md5_chan, to_update_xml, closed_chan := NewChannels()

	// Now get the json manager
	// We only have one of these to make file locks easier
	// This receives a file structure on one channel, opens that file and modifies it accordingly.
	// In the final application this will be the only thing that can update the xml files
	wg := NewXmlManager(to_update_xml)

	// TBD do something better here
	to_md5_chan <- FileStruct{Name: "bob", directory: "."}
	log.Println("Sent the file to check")
	close(to_md5_chan)
	log.Println("Waiting for channel to close")
	<-closed_chan
	log.Println("Channel Closed, waiting for XmlManager to complete")
	wg.Wait()
	log.Println("All done")
}
func TestSelfCompat(t *testing.T) {
	fileToUse := "checksum_test.go"
	removeMd5(".")

	dm := *NewDirectoryMap()
	to_md5_chan, to_update_xml, closed_chan := NewChannels()
	wg := NewXmlManager(to_update_xml)
	to_md5_chan <- FileStruct{Name: fileToUse, directory: "."}
	close(to_md5_chan)
	<-closed_chan
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
func TestBuildAll(t *testing.T) {
	directory := "."
	fileToUse := "checksum_test.go"
	removeMd5(".")

	UpdateDirectory(directory)

	dm := DirectoryMapFromDir(".")
	_, ok := dm.Get(fileToUse)
	if !ok {
		log.Fatal(fileToUse, " is gone!!!", dm)
	}
	removeMd5(".")

}
func TestPerlCompat(t *testing.T) {
	perlScript := "/home/cbh/home/script/perl/file_check.pl"
	fileToUse := "checksum_test.go"
	if _, err := os.Stat("./" + Md5FileName); os.IsExist(err) {
		os.Remove("./" + Md5FileName)
	}

	cmd := exec.Command(perlScript, ".")
	err := cmd.Run()
	if err != nil {
		log.Fatal("Erred!", err)
	}

	dm := DirectoryMapFromDir(".")
	v, ok := dm.Get(fileToUse)
	if !ok {
		log.Fatal(fileToUse, " is gone!", dm)
	}
	checksum := v.Checksum
	if checksum == "" {
		log.Fatal("Missing Checksum from perl version")
	}
	os.Remove("./" + Md5FileName)
	dm = *NewDirectoryMap()
	to_md5_chan, to_update_xml, closed_chan := NewChannels()
	wg := NewXmlManager(to_update_xml)
	to_md5_chan <- FileStruct{Name: fileToUse, directory: "."}
	close(to_md5_chan)
	<-closed_chan
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
