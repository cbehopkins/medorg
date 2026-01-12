package core

import (
	"os"
	"path/filepath"
	"strings"
)

// Fname is used to indicate we are talking about a filename (not a path)
type Fname string

// Dirname is used to indicate we are talking about a directory path (not a full file path)
type Dirname string

// Fpath is used to indicate we are talking about the full file path
type Fpath string

func (f Fpath) String() string {
	return string(f)
}
func (f Fpath) Dir() Dirname {
	return Dirname(filepath.Dir(string(f)))
}
func (f Fpath) Base() Fname {
	return Fname(filepath.Base(string(f)))
}


func NewFpath(directory, fn string) Fpath {
	return Fpath(filepath.Join(directory, fn))
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
	pa := strings.Split(path, string(filepath.Separator))

	for _, p := range pa {
		if strings.HasPrefix(p, ".") {
			return true
		}
	}
	return false
}
func hasSkipfile(directory string) bool {
	skipFilePath := filepath.Join(directory, ".mdSkipDir")
	if _, err := os.Stat(skipFilePath); !os.IsNotExist(err) {
		return true
	}
	return false
}
