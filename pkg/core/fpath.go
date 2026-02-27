package core

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// Md5FileName is the filename we use to save the data in
const (
	/// The File Names for:
	Md5FileName     = ".medorg.xml"    // md5 checksum/backupdest/tag data
	ConfigFileName  = ".mdcfg.xml"     // medorg configuration data
	JournalPathName = ".mdjournal.xml" // journal data
	VolumePathName  = ".mdbackup.xml"  // volume backup data - i.e. what is written in the root of a backup volume
	SkipDirFile     = ".mdSkipDir"     // presence of this file causes medorg to skip the directory
)

func IsMetadataFile(fn string) bool {
	return fn == Md5FileName || fn == ConfigFileName || fn == JournalPathName || fn == VolumePathName || fn == SkipDirFile
}

// Fname is used to indicate we are talking about a filename (not a path)
type Fname string

// Dirname is used to indicate we are talking about a directory path (not a full file path)
type Dirname string

// Fpath is used to indicate we are talking about the full file path
type Fpath struct {
	string
	dir  *Dirname
	base *Fname
}

func (f Fpath) Is(name string) bool {
	return strings.EqualFold(string(f.Base()), name)
}
func (f Fpath) String() string {
	return f.string
}
func (f Fpath) Dir() Dirname {
	if f.dir == nil {
		d := Dirname(filepath.Dir(f.string))
		f.dir = &d
	}

	return *f.dir
}
func (f Fpath) Base() Fname {
	if f.base == nil {
		b := Fname(filepath.Base(f.string))
		f.base = &b
	}
	return *f.base
}
func (f Fpath) IsMetadataFile() bool {
	return IsMetadataFile(string(f.Base()))
}

func NewFpath(parts ...any) Fpath {
	if len(parts) < 1 || len(parts) > 2 {
		panic("NewFpath requires 1 or 2 arguments")
	}

	// Convert each part to string
	strs := make([]string, len(parts))
	for i, part := range parts {
		switch v := part.(type) {
		case string:
			strs[i] = v
		case Dirname:
			strs[i] = string(v)
		case Fname:
			strs[i] = string(v)
		case Fpath:
			strs[i] = v.string
		case fmt.Stringer:
			strs[i] = v.String()
		default:
			panic("NewFpath arguments must be string or have a String() method")
		}
	}

	if len(strs) == 1 {
		return Fpath{string: strs[0]}
	}
	return Fpath{string: filepath.Join(strs[0], strs[1])}
}

func isHiddenDirectory(path string) bool {
	if path == "." || path == ".." {
		return false
	}
	stat, err := os.Stat(path)
	if err != nil {
		return false
	}
	if !stat.IsDir() {
		path, _ = filepath.Split(path)
	}
	path = filepath.Clean(path)
	pa := strings.SplitSeq(path, string(filepath.Separator))

	for p := range pa {
		if strings.HasPrefix(p, ".") {
			return true
		}
	}
	return false
}
func hasSkipfile(directory string) bool {
	skipFilePath := filepath.Join(directory, SkipDirFile)
	if _, err := os.Stat(skipFilePath); !os.IsNotExist(err) {
		return true
	}
	return false
}

// hasSkipfileInEntries checks if the skip file exists in the provided directory entries
// This avoids an additional os.Stat call when we already have the directory entries
func hasSkipfileInEntries(entries []fs.DirEntry) bool {
	for _, entry := range entries {
		if entry.Name() == SkipDirFile {
			return true
		}
	}
	return false
}
