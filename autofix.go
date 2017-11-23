package medorg

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
)

var KnownExtensions = []string{
	"go",
	"jpg",
	"flv", "mov", "mp4", "mpg",
}

func (af *AutoFix) afInit(dl []string) {
	af.ReStaticNum = regexp.MustCompile("(.*)(\\(\\d+\\))$")
	af.ReDomainList = make([]*regexp.Regexp, len(dl))
	if len(dl) == 0 {
		log.Fatal("Unexpected init order")
	}
	for i, rs := range dl {
		af.ReDomainList[i] = regexp.MustCompile(rs)
	}
}

type AutoFix struct {
	DeleteFiles  bool
	RenameFiles  bool
	ReStaticNum  *regexp.Regexp
	ReDomainList []*regexp.Regexp
}

func NewAutoFix(dl []string) *AutoFix {
	itm := new(AutoFix)
	itm.afInit(dl)
	return itm
}

func stripExtension(fn string) (base, extension string) {
	for _, ext := range KnownExtensions {
		withDot := "." + ext
		if strings.HasSuffix(fn, withDot) {
			s := strings.TrimSuffix(fn, withDot)
			return s, withDot
		}
	}
	return fn, ""
}

// isXDirectory tests if the directory specified
// forms part of the path
// i.e. we're looking for .../x/...
// acknowledging we could have .../x
// or .../xy/... etc
func isXDirectory(dir, x string) bool {
	if !strings.Contains(dir, x) {
		return false
	}
	dir = strings.TrimSuffix(dir, "/")
	if strings.HasSuffix(dir, "/"+x) {
		return true
	}
	if strings.Contains(dir, "/"+x+"/") {
		return true
	}
	return false
}

// scoreName for relative merit to another
func scoreName(dir0, fn0, dir1, fn1 string) (score int) {
	// Some rules (+/- indicate ggod or bad for that file)
	// A longer directory name implies it is more sorted ++
	// being in the "to" directory implies it is unsorted --
	// A longer name is discouraged -
	// being in a favs directory is very sorted +++
	if len(dir0) > len(dir1) {
		score += 2
	}
	if isXDirectory(dir0, "to") {
		score -= 2
	}
	if len(fn0) > len(fn1) {
		score -= 1
	}
	if isXDirectory(dir0, "favs") {
		score += 2
	}
	return
}

// True if the first file doesn't have the largest score
func swapFile(score1, score2 int) bool {
	return score2 > score1
}

func (af AutoFix) ResolveTwo(fsOne, fsTwo FileStruct) (FileStruct, bool) {
	fmt.Println("Matching Files", fsOne, fsTwo)

	score1 := scoreName(fsOne.Directory(), fsOne.Name, fsTwo.Directory(), fsTwo.Name)
	score2 := scoreName(fsTwo.Directory(), fsTwo.Name, fsOne.Directory(), fsOne.Name)

	if swapFile(score1, score2) {
		log.Println("File:", fsTwo, "Preferred over:", fsOne)
		fsOne, fsTwo = fsTwo, fsOne
	}

	if af.DeleteFiles {
		// Delete the file we don's want
		// By definuition that's the second one
	}
	return fsOne, false
}

func (af AutoFix) stripNumber(fn string) (string, bool) {
	// Here we're looking for the pattern (\d+) on filenames
	strA := af.ReStaticNum.FindStringSubmatch(fn)
	if len(strA) == 3 {
		return strA[1], true
	}
	return fn, false
}
func (af AutoFix) stripDomains(fn string) (string, bool) {
	for _, re := range af.ReDomainList {
		strA := re.FindStringSubmatch(fn)
		if len(strA) == 2 {
			return strA[1], true
		}
	}
	return fn, false
}
func potentialFilename(directory, fn, extension string, i int) (string, bool) {
	potentialFn := fn + "(" + strconv.Itoa(i) + ")" + extension
	return potentialFn, FileExist(directory, potentialFn)
}
func (af AutoFix) CheckRename(fs FileStruct) (FileStruct, bool) {
	var modified bool
	var mod bool
	directory := fs.Directory()
	// Test to see if it matches one of the patterns and modify it

	fsNew := fs

	// If what we would like to call it already exists
	// Rewrite the name to be a non-conflicting (n) format
	base, extension := stripExtension(fs.Name)
	if extension == "" {
		// Do nothing for files we don't recognise
		return fs, false
	}
	base, mod = af.stripNumber(base)
	modified = modified || mod
	base, mod = af.stripDomains(base)
	modified = modified || mod

	if !modified {
		// Changed nothing, so go no further
		return fs, false
	}

	pfn := base + extension
	if FileExist(directory, pfn) {
		exist := true
		for i := 0; exist; i++ {
			pfn, exist = potentialFilename(directory, base, extension, i)
		}
	}
	fsNew.Name = pfn
	if fsNew.Name != fs.Name {
		if af.RenameFiles {
			fsNew.Name = base + extension
			MoveFile(directory+"/"+fs.Name, directory+"/"+fsNew.Name)
			return fsNew, true
		}
		log.Println("Rename:", fs.Name, " to ", fsNew.Name)
	}
	return fs, false
}
