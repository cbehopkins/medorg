package core

import (
	"os"
	"path/filepath"
	"strings"
)

// Fpath is used to indicate we are talking about the full file path
type Fpath string

func (f Fpath) String() string {
	return string(f)
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
