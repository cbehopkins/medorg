package medorg

import "path/filepath"

// fpath is used to indicate we are talking about the full file path
type fpath string

func (f fpath) String() string {
	return string(f)
}
func Fpath(directory, fn string) fpath {
	return fpath(filepath.Join(directory, fn))
}

type fpathList []fpath

func (fpl *fpathList) Add(fp fpath) {
	*fpl = append(*fpl, fp)
}

// FIXME add a "I have reached n items in my list, therefore I will discard further additions"
// And mark for future, that I have done this (Perhaps with a callback?)
// To have a method of limiting memory usage
type fpathListList []fpathList

func (fpll *fpathListList) Add(index int, fp fpath) {
	for len(*fpll) <= index {
		// append until we have a list that is long enough
		// TBD potentially add several in one go
		*fpll = append(*fpll, fpathList{})
	}

	(*fpll)[index].Add(fp)
}
