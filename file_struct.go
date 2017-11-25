package medorg

import (
	"log"
	"os"
)

// FileStruct contains all the properties associated with a file
type FileStruct struct {
	XMLName   struct{} `xml:"fr"`
	directory string   // Kept as hidden from the xml as this is used for messaging between agents
	// and that does not want to end up in the final xml file
	Name     string `xml:"fname,attr"`
	Checksum string `xml:"checksum,attr"`

	Mtime int64    `xml:"mtime,attr,omitempty"`
	Size  int64    `xml:"size,attr,omitempty"`
	Tags  []string `xml:"tags,omitempty"`
}

func (fs FileStruct) String() string {
	retStr := "[FileStruct]{"
	if fs.directory != "" {
		retStr += "directory:\"" + fs.directory + "\""
	}
	retStr += "Name:\"" + fs.Name + "\""
	retStr += "Checksum:" + fs.Checksum + "\""
	retStr += "}"

	return retStr
}

// Directory return the directory the file is in
func (fs FileStruct) Directory() string {
	return fs.directory
}

// Path return the path of the file
func (fs FileStruct) Path() string {
	return fs.directory + "/" + fs.Name
}

// FsFromName creates a file struct from the supplied name
func FsFromName(directory, fn string) FileStruct {
	fp := directory + "/" + fn
	fs, err := os.Stat(fp)

	if os.IsNotExist(err) {
		log.Fatal("Asked to create a fs for a file that does not exist", fp)
	}

	itm := new(FileStruct)
	itm.Name = fn
	itm.Mtime = fs.ModTime().Unix()
	itm.Size = fs.Size()
	itm.directory = directory
	if Debug {
		log.Println("New FS for file", fp, "Size:", itm.Size, " Time:", itm.Mtime)
	}
	return *itm
}

func (fs FileStruct) checkDelete(directory, fn string) bool {
	if (directory != fs.directory) || (fn != fs.Name) {
		// TBD this should be able to be removed if tests prove good
		log.Fatal("Mismatch in expected directoty and name")
	}
	//fp := directory + "/" + fn
	fp := fs.Path()
	if fn == "" {
		if Debug {
			log.Println("Blank filename in xml", fp)
		}
		return true
	}
	// for each file, check if it exists
	if fstat, err := os.Stat(fp); os.IsNotExist(err) {
		// if it does not, remove from the map
		//fmt.Println("Removing XML entry as file not exist", fn)
		return true
	} else if os.IsExist(err) || (err == nil) {
		// If it does, then check if the attributes are accurate
		ftD := fstat.ModTime().Unix()
		szD := fstat.Size()
		ftX := fs.Mtime
		szX := fs.Size

		if ftD != ftX {
			log.Println("File times for ", fp, "do not match. File:", ftD, "Xml:", ftX)
			return true
		}
		if szD != szX {
			log.Println("Sizes for ", fp, "do not match. File:", szD, "Xml:", szX)
			return true
		}
		return false
	} else {
		log.Fatal("A file that neither exists, nor doesn't exist", err)
	}
	return false
}
