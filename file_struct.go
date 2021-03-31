package medorg

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
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
	Analysed   int64    `xml:"analysed,omitempty"`
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
func (fs FileStruct) Path() fpath {
	return Fpath(fs.directory, fs.Name)
}

// Equal return the path of the file
func (fs FileStruct) Equal(ca FileStruct) bool {
	return (fs.Size == ca.Size) && (fs.Checksum == ca.Checksum)
}

func NewFileStruct(directory string, fn string) (*FileStruct, error) {
	fp := filepath.Join(directory, fn)
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
	fp := string(fs.Path())
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

var ErrSameFileMtime = errors.New("they do not have the same Mtime")
var ErrSameFiledirectory = errors.New("they do not have the same directory")
var ErrSameFileSize = errors.New("they do not have the same Size")
var ErrSameFileName = errors.New("they do not have the same Name")

func (fs FileStruct) SameFileFast(fs_i FileStruct) error {
	if fs.Name != fs_i.Name {
		return ErrSameFileName
	}

	if fs.Size != fs_i.Size {
		return ErrSameFileSize
	}

	if fs.directory != fs_i.directory {
		return ErrSameFiledirectory
	}

	if fs.Mtime != fs_i.Mtime {
		return ErrSameFileMtime
	}
	return nil
}

func (fs FileStruct) SameFile(fs_i FileStruct) error {
	if fs.Name != fs_i.Name {
		return fmt.Errorf("%w: %v, %v", ErrSameFileName, fs.Name, fs_i.Name)
	}

	if fs.Size != fs_i.Size {
		return fmt.Errorf("%w: %v, %v", ErrSameFileSize, fs.Size, fs_i.Size)
	}

	if fs.directory != fs_i.directory {
		return fmt.Errorf("%w: %v, %v", ErrSameFiledirectory, fs.directory, fs_i.directory)
	}

	if fs.Mtime != fs_i.Mtime {
		return fmt.Errorf("%w: %v, %v", ErrSameFileMtime, fs.Mtime, fs_i.Mtime)
	}
	return nil
}

// HasTag return true is the tag is already in ArchivedAt
func (fs FileStruct) HasTag(tag string) bool {
	for _, v := range fs.ArchivedAt {
		if v == tag {
			return true
		}
	}
	return false
}

// Add a tag to the fs, return true if it was modified
func (fs *FileStruct) AddTag(tag string) bool {
	if fs.HasTag(tag) {
		return false
	}
	fs.ArchivedAt = append(fs.ArchivedAt, tag)
	return true
}
