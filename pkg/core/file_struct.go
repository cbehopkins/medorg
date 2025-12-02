package core

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

var ErrRecalced = errors.New("File checksum has been recalculated")

// FileStruct contains all the properties associated with a file
type FileStruct struct {
	XMLName   struct{} `xml:"fr"`
	directory string   // Kept as hidden from the xml as this is used for messaging between agents
	// and that does not want to end up in the final xml file
	Name     string `xml:"fname,attr"`
	Checksum string `xml:"checksum,attr"`

	Mtime      int64    `xml:"mtime,attr,omitempty"`
	Size       int64    `xml:"size,attr"`
	Tags       []string `xml:"tag,omitempty"`
	BackupDest []string `xml:"bd,omitempty"`
}

// FileStructArray declares an array of filestructs, explicitly for sorting
type FileStructArray []FileStruct

// Len for sorting
func (fsa FileStructArray) Len() int {
	return len(fsa)
}

// Swap for sorting
func (fsa FileStructArray) Swap(i, j int) {
	fsa[i], fsa[j] = fsa[j], fsa[i]
}

// Less for sorting
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

// SetDirectory sets the directory the file is in
func (fs *FileStruct) SetDirectory(directory string) {
	fs.directory = directory
}

// Path return the path of the file
func (fs FileStruct) Path() Fpath {
	return NewFpath(fs.directory, fs.Name)
}

// Key to use when indexing into map for comparisons
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

// NewFileStruct returns a populated file struct with
// the file properties set as read from file
func NewFileStruct(directory string, fn string) (fs FileStruct, err error) {
	fp := filepath.Join(directory, fn)
	stat, err := os.Stat(fp)
	if err != nil {
		return fs, err
	}

	return fs.FromStat(directory, fn, stat)
}

// FromStat update the file struct from a supplied file structure
func (fs *FileStruct) FromStat(directory string, fn string, fsi os.FileInfo) (FileStruct, error) {
	if changed, err := fs.Changed(fsi); !changed {
		return *fs, err
	}
	fs.Name = fn
	fs.Mtime = fsi.ModTime().Unix()
	fs.Size = fsi.Size()
	fs.Checksum = ""
	fs.BackupDest = []string{}
	fs.directory = directory
	return *fs, nil
}

func (fs FileStruct) indexTag(tag string) int {
	for i, v := range fs.BackupDest {
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
	fs.BackupDest = append(fs.BackupDest, tag)
	return true
}

// Remove a tag from the fs, return true if it was modified
func (fs *FileStruct) RemoveTag(tag string) bool {
	index := fs.indexTag(tag)
	if index < 0 {
		return false
	}
	// Order is not important, so swap interesting element to the end and remove
	fs.BackupDest[len(fs.BackupDest)-1], fs.BackupDest[index] = fs.BackupDest[index], fs.BackupDest[len(fs.BackupDest)-1]
	fs.BackupDest = fs.BackupDest[:len(fs.BackupDest)-1]
	return true
}

// Changed reports if the filestruct has changed from the supplied info
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

// UpdateChecksum makes the tea
func (fs *FileStruct) UpdateChecksum(forceUpdate bool) error {
	if !forceUpdate && (fs.Checksum != "") {
		return nil
	}
	cks, err := CalcMd5File(fs.directory, fs.Name)
	if err != nil {
		return err
	}
	if fs.Checksum == cks {
		return nil
	}
	fs.Checksum = cks
	// If we've had to update the checksum, then any existing backups are invalid
	fs.BackupDest = []string{}
	return nil
}

// ValidateChecksum checks if the checksum is correct
func (fs *FileStruct) ValidateChecksum() error {
	cks, err := CalcMd5File(fs.directory, fs.Name)
	if err != nil {
		return err
	}
	if fs.Checksum == cks {
		return nil
	}
	fs.Checksum = cks
	// If we've had to update the checksum, then any existing backups are invalid
	fs.BackupDest = []string{}
	return ErrRecalced
}

// Interface implementation - helper methods for interface support

// BackupDestinations returns all backup volume labels where this file exists
func (fs FileStruct) BackupDestinations() []string {
	return fs.BackupDest
}

// AddBackupDestination adds a backup volume label
func (fs *FileStruct) AddBackupDestination(label string) {
	fs.AddTag(label)
}

// HasBackupOn checks if backed up to a specific volume
func (fs FileStruct) HasBackupOn(label string) bool {
	return fs.HasTag(label)
}

// Accessor methods for interface compatibility
// These allow FileMetadata interface users to access field values

// GetSize returns the file size
func (fs FileStruct) GetSize() int64 {
	return fs.Size
}

// GetChecksum returns the file checksum
func (fs FileStruct) GetChecksum() string {
	return fs.Checksum
}

// GetTags returns a copy of the tags slice
func (fs FileStruct) GetTags() []string {
	return append([]string{}, fs.Tags...)
}

// GetName returns the filename
func (fs FileStruct) GetName() string {
	return fs.Name
}
