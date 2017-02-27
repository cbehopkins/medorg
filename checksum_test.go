package medorg

import (
	"io/ioutil"
	"log"
	"testing"
)

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
	fred.UnmarshalXml(marshelled)
	log.Println(fred)

	log.Println("All Done")
}

func TestMd5(t *testing.T) {
	// Check the MD5 creation mechanism

	// So get us a channel to send the files to be md5'd to
	// This returns 2 channels, one that files to be checked should be sent to
	// a second that gives the filename and checksum (sent as a file object)
	to_md5_chan, to_update_xml := NewChannels()

	// Now get the json manager
	// We only have one of these to make file locks easier
	// This receives a file structure on one channel, opens that file and modifies it accordingly.
	// In the final application this will be the only thing that can update the xml files
	NewXmlManager(to_update_xml)

	// TBD do something better here
	to_md5_chan <- FileStruct{Filename: "bob", directory: "."}
	close(to_md5_chan)

}
