package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/cbehopkins/medorg"
)

var FileHash map[string]medorg.FileStruct
var DeleteFiles bool
var RenameFiles bool

var KnownExtensions = []string{
	"go",
	"jpg",
	"flv", "mov", "mp4", "mpg",
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

func resolveTwo(fsOne, fsTwo medorg.FileStruct) (medorg.FileStruct, bool) {
	fmt.Println("Matching Files", fsOne, fsTwo)

	score1 := scoreName(fsOne.Directory(), fsOne.Name, fsTwo.Directory(), fsTwo.Name)
	score2 := scoreName(fsTwo.Directory(), fsTwo.Name, fsOne.Directory(), fsOne.Name)

	if swapFile(score1, score2) {
		log.Println("File:", fsTwo, "Preferred over:", fsOne)
		fsOne, fsTwo = fsTwo, fsOne
	}

	if DeleteFiles {
		// Delete the file we don's want
		// By definuition that's the second one
	}
	return fsOne, false
}

var ReStaticNum *regexp.Regexp

// TBD Populate this from a file
var DomainList = []string{"(.*)_calc"}
var ReDomainList []*regexp.Regexp

func init() {
	ReStaticNum = regexp.MustCompile("(.*)(\\(\\d+\\))$")
	ReDomainList = make([]*regexp.Regexp, len(DomainList))
	if len(DomainList) == 0 {
		log.Fatal("Unexpected init order")
	}
	for i, rs := range DomainList {
		ReDomainList[i] = regexp.MustCompile(rs)
	}
}
func stripNumber(fn string) (string, bool) {
	// Here we're looking for the pattern (\d+) on filenames
	strA := ReStaticNum.FindStringSubmatch(fn)
	if len(strA) == 3 {
		return strA[1], true
	}
	return fn, false
}
func stripDomains(fn string) (string, bool) {
	for _, re := range ReDomainList {
		strA := re.FindStringSubmatch(fn)
		if len(strA) == 2 {
			return strA[1], true
		}
	}
	return fn, false
}
func potentialFilename(directory, fn, extension string, i int) (string, bool) {
	potentialFn := fn + "(" + strconv.Itoa(i) + ")" + extension
	return potentialFn, medorg.FileExist(directory, potentialFn)
}
func checkRename(fs medorg.FileStruct) (medorg.FileStruct, bool) {
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
	base, mod = stripNumber(base)
	modified = modified || mod
	base, mod = stripDomains(base)
	modified = modified || mod

	if !modified {
		// Changed nothing, so go no further
		return fs, false
	}

	pfn := base + extension
	if medorg.FileExist(directory, pfn) {
		exist := true
		for i := 0; exist; i++ {
			pfn, exist = potentialFilename(directory, base, extension, i)
		}
	}
	fsNew.Name = pfn
	if fsNew.Name != fs.Name {
		if RenameFiles {
			fsNew.Name = base + extension
			medorg.MoveFile(directory+"/"+fs.Name, directory+"/"+fsNew.Name)
			return fsNew, true
		}
		log.Println("Rename:", fs.Name, " to ", fsNew.Name)
	}
	return fs, false
}
func wkFun(directory, fn string, fs medorg.FileStruct, dm *medorg.DirectoryMap) bool {
	var modified bool
	if fs.Directory() != directory {
		log.Fatal("Structure Problem for", directory, fn)
	}
	if fs.Size == 0 {
		fmt.Println("Zero Length File")
		if DeleteFiles {
			err := dm.RmFile(directory, fn)
			if err != nil {
				log.Fatal("Couldn't delete file", directory, fn)
			}
		}
		return true
	}
	// now look to see if we should rename the file
	var mod bool
	fs, mod = checkRename(fs)
	modified = modified || mod

	// Check if two of the checksums are equal
	cSum := fs.Checksum
	oldFs, ok := FileHash[cSum]
	if ok {
		fs, mod = resolveTwo(fs, oldFs)
		modified = modified || mod
	}

	FileHash[cSum] = fs
	// Return true when we modify dm
	return modified
}

// after we have finished in a directory and written out the dm
// this is called
func drFun(directory string, dm *medorg.DirectoryMap) {
}

// Our master Modification func
// This is called on every file
// We are allowed to modify the fs that will be added
// We are not allowed to delete it
// More because during this phase other xmls may be open
// so we can't modify those
func masterMod(dir, fn string, fs medorg.FileStruct) (medorg.FileStruct, bool) {
	return fs, false
}
func isDir(fn string) bool {
	stat, err := os.Stat(fn)
	if os.IsNotExist(err) {
		return false
	}
	if os.IsExist(err) || err == nil {
		if stat.IsDir() {
			return true
		}
	}
	return false
}
func main() {
	var directories []string
	FileHash = make(map[string]medorg.FileStruct)
	var walkCnt = flag.Int("walk", 2, "Max Number of directory Walkers")
	var calcCnt = flag.Int("calc", 2, "Max Number of MD5 calculators")
	flag.Parse()

	if flag.NArg() > 0 {
		for _, fl := range flag.Args() {
			if isDir(fl) {
				directories = append(directories, fl)
			}
		}
	} else {
		directories = []string{"."}
	}

	// Subtle - we want the walk engine to be able to start a calc routing
	// without that calc routine having a token as yet
	// i.e. we want the go scheduler to have some things queued up to do
	// This allows us to set calcCnt to the amount of IO we want
	// and walkCnt to be set to allow the directory structs to be hammered
	pendCnt := *calcCnt + *walkCnt
	for _, directory := range directories {
		tu := medorg.NewTreeUpdate(*walkCnt, *calcCnt, pendCnt)

		tu.UpdateDirectory(directory, masterMod)
		tw := medorg.NewTreeWalker()
		tw.WalkTree(directory, wkFun, drFun)
	}
}
