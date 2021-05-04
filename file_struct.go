package medorg

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
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
type FileStructArray []FileStruct

func (fsa FileStructArray) Len() int {
	return len(fsa)
}
func (fsa FileStructArray) Swap(i, j int) {
	fsa[i], fsa[j] = fsa[j], fsa[i]
}
func (fsa FileStructArray) Less(i, j int) bool {
	// REVISIT!
	return strings.Compare(fsa[i].Name, fsa[j].Name) == -1
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
func (fs FileStruct) Path() Fpath {
	return NewFpath(fs.directory, fs.Name)
}
func (fs FileStruct) Key() backupKey {
	return backupKey{fs.Size, fs.Checksum}
}

// Equal test two file structs to see if we consider them equivalent
func (fs FileStruct) Equal(ca FileStruct) bool {
	if fs.Checksum == "" || ca.Checksum == "" {
		return false
	}
	return (fs.Size == ca.Size) && (fs.Checksum == ca.Checksum)
}

func NewFileStruct(directory string, fn string) (fs FileStruct, err error) {
	fp := filepath.Join(directory, fn)
	stat, err := os.Stat(fp)
	if err != nil {
		return fs, err
	}

	return fs.FromStat(directory, fn, stat)
}

func (fs *FileStruct) FromStat(directory string, fn string, fsi os.FileInfo) (FileStruct, error) {
	if changed, err := fs.Changed(fsi); !changed {
		return *fs, err
	}
	fs.Name = fn
	fs.Mtime = fsi.ModTime().Unix()
	fs.Size = fsi.Size()
	fs.Checksum = "" //
	fs.ArchivedAt = []string{}
	fs.directory = directory
	return *fs, nil
}

func (fs FileStruct) indexTag(tag string) int {
	for i, v := range fs.ArchivedAt {
		if v == tag {
			return i
		}
	}
	return -1
}

// HasTag return true is the tag is already in ArchivedAt
func (fs FileStruct) HasTag(tag string) bool {
	return fs.indexTag(tag) >= 0
}

// Add a tag to the fs, return true if it was modified
func (fs *FileStruct) AddTag(tag string) bool {
	if fs.HasTag(tag) {
		return false
	}
	fs.ArchivedAt = append(fs.ArchivedAt, tag)
	return true
}

// Remove a tag from the fs, return true if it was modified
func (fs *FileStruct) RemoveTag(tag string) bool {
	index := fs.indexTag(tag)
	if index < 0 {
		return false
	}
	// Order is not important, so swap interesting element to the end and remove
	fs.ArchivedAt[len(fs.ArchivedAt)-1], fs.ArchivedAt[index] = fs.ArchivedAt[index], fs.ArchivedAt[len(fs.ArchivedAt)-1]
	fs.ArchivedAt = fs.ArchivedAt[:len(fs.ArchivedAt)-1]
	return true
}

func (fs FileStruct) Changed(info fs.FileInfo) (bool, error) {
	if info == nil {
		return false, errors.New("changed called on nil fileinfo")
	}
	if fs.Mtime != info.ModTime().Unix() {
		return true, nil
	}
	if fs.Size != info.Size() {
		return true, nil
	}
	return false, nil
}

func (fs *FileStruct) UpdateChecksum(forceUpdate bool) error {
	if !forceUpdate && (fs.Checksum != "") {
		return nil
	}
	// FIXME we need a compute token for this
	cks, err := CalcMd5File(fs.directory, fs.Name)
	if err != nil {
		return err
	}
	if fs.Checksum == cks {
		return nil
	}
	// log.Println("Recalculation of ", file, "found a changed checksum")
	fs.Checksum = cks
	fs.ArchivedAt = []string{}
	return nil
}
