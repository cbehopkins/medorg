package medorg

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

type fpathList []Fpath

func (fpl *fpathList) Add(fp Fpath) {
	*fpl = append(*fpl, fp)
}

// FIXME add a "I have reached n items in my list, therefore I will discard further additions"
// And mark for future, that I have done this (Perhaps with a callback?)
// To have a method of limiting memory usage
type fpathListList []fpathList

func (fpll *fpathListList) Add(index int, fp Fpath) {
	for len(*fpll) <= index {
		// append until we have a list that is long enough
		// TBD potentially add several in one go
		*fpll = append(*fpll, fpathList{})
	}

	(*fpll)[index].Add(fp)
}
func isChildPath(ref, candidate string) (bool, error) {

	rp, err := filepath.Abs(ref)
	if err != nil {
		return false, err
	}
	can, err := filepath.Abs(candidate)
	if err != nil {
		return false, err
	}

	return strings.Contains(rp, can), nil
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
