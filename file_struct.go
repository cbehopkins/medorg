package medorg

import (
	"errors"
	"fmt"
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

	Mtime      int64    `xml:"mtime,attr,omitempty"`
	Size       int64    `xml:"size,attr,omitempty"`
	Analysed   int      `xml:"analysed,omitempty"`
	Tags       []string `xml:"tags,omitempty"`
	ArchivedAt []string `xml:"ArchivedAt,omitempty"`
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

func NewFileStruct(directory string, fn string) (*FileStruct, error) {
	fp := directory + "/" + fn
	fs, err := os.Stat(fp)
	if err != nil {
		return nil, err
	}
	return NewFileStructFromStat(directory, fn, fs)
}

func NewFileStructFromStat(directory string, fn string, fs os.FileInfo) (*FileStruct, error) {
	itm := new(FileStruct)
	itm.Name = fn
	itm.Mtime = fs.ModTime().Unix()
	itm.Size = fs.Size()
	itm.directory = directory
	return itm, nil
}

func (fs FileStruct) checkDelete(directory, fn string) bool {
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

var SameFileErrMtime = errors.New("They do not have the same Mtime")
var SameFileErrdirectory = errors.New("They do not have the same directory")
var SameFileErrSize = errors.New("They do not have the same Size")
var SameFileErrName = errors.New("They do not have the same Name")

func (fs FileStruct) SameFile(fs_i FileStruct) error {
	if fs.Name != fs_i.Name {
		return fmt.Errorf("%w: %v, %v", SameFileErrName, fs.Name, fs_i.Name)
	}

	if fs.Size != fs_i.Size {
		return fmt.Errorf("%w: %v, %v", SameFileErrSize, fs.Size, fs_i.Size)
	}

	if fs.directory != fs_i.directory {
		return fmt.Errorf("%w: %v, %v", SameFileErrdirectory, fs.directory, fs_i.directory)
	}

	if fs.Mtime != fs_i.Mtime {
		return fmt.Errorf("%w: %v, %v", SameFileErrMtime, fs.Mtime, fs_i.Mtime)
	}
	return nil
}
