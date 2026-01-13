package core

import (
	"fmt"
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
			strs[i] = string(v)
		case fmt.Stringer:
			strs[i] = v.String()
		default:
			panic("NewFpath arguments must be string or have a String() method")
		}
	}

	if len(strs) == 1 {
		return Fpath(strs[0])
	}
	return Fpath(filepath.Join(strs[0], strs[1]))
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
