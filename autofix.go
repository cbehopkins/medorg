package medorg

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

var testMode bool

// KnownExtensions is a list of extensions we are allowed to operate on
var KnownExtensions = []string{
	"go",
	"jpg", "gif", "jpeg",
	"wmv", "flv", "mov", "mp4", "mpg",
}

// AutoFix is the structure for autofixing the files
type AutoFix struct {
	DeleteFiles  bool
	RenameFiles  bool
	ReStaticNum  *regexp.Regexp
	ReDomainList []*regexp.Regexp
	// For all the file we encounter, keep their hash
	// FIXME elsewhere we use size && hash as the key to uniqueness
	FileHash       map[string]FileStruct
	FhLock         *sync.RWMutex
	SilenceLogging bool
}

// initFileHash needs to be run before we can use the master checkers
func (af *AutoFix) initFileHash() {
	if af.FhLock == nil {
		af.FhLock = new(sync.RWMutex)
	}
	if af.FileHash == nil {
		af.FileHash = make(map[string]FileStruct)
	}
}

// NewAutoFix reads in descriptions from an array
func NewAutoFix(dl []string) *AutoFix {
	itm := new(AutoFix)
	itm.afInit(dl)
	itm.initFileHash()
	return itm
}

// NewAutoFixFile reads in autofix expressions from a file
func NewAutoFixFile(fn string) *AutoFix {
	var dl []string
	// FIXME 10/10 for good intentions minus several million for implementation
	// We load a line at a time, just to put it all into an array!
	for s := range LoadFile(fn) {
		dl = append(dl, s)
	}
	return NewAutoFix(dl)
}

func (af *AutoFix) afInit(dl []string) {
	af.ReStaticNum = regexp.MustCompile(`(.*)(\(\d+\))$`)
	af.ReDomainList = make([]*regexp.Regexp, len(dl))
	for i, rs := range dl {
		af.ReDomainList[i] = regexp.MustCompile(rs)
	}
}

// StripExtension removes off any known extensions and returns it with modified filename
func StripExtension(fn string) (base, extension string) {
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
	// Some rules (+/- indicate good or bad for that file)
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
		score--
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

// ResolveTwo we have 2 equivalent files
// Return the one that should remaion
// return if change  to first one has been made
// delete if configured
func (af AutoFix) ResolveTwo(fsOne, fsTwo FileStruct) (FileStruct, bool) {
	if Debug {
		fmt.Println("Matching Files", fsOne, fsTwo)
	}

	score1 := scoreName(fsOne.Directory(), fsOne.Name, fsTwo.Directory(), fsTwo.Name)
	score2 := scoreName(fsTwo.Directory(), fsTwo.Name, fsOne.Directory(), fsOne.Name)

	//log.Println("Score1:", score1,"Score2:", score2)

	if swapFile(score1, score2) {
		if Debug {
			log.Println("File:", fsTwo, "Preferred over:", fsOne)
		}
		fsOne, fsTwo = fsTwo, fsOne
	}

	if af.DeleteFiles {
		// Delete the file we don't want
		// By definuition that's the second one
		fn := fsTwo.Path()
		log.Println("Deleting:", fn)
		_ = rmFilename(fn)
	} else if !af.SilenceLogging {
		log.Println("Delete:", fsTwo.Path(), " as ", fsOne.Path())
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
func (af AutoFix) stripRegEx(fn string) (newFn string, modified bool) {
	newFn = fn
	doWork := true
	for doWork {
		doWork = false
		var modifiedInner bool
		for _, re := range af.ReDomainList {
			strA := re.FindStringSubmatch(newFn)
			if len(strA) == 2 {
				modifiedInner = true
				newFn = strA[1]
			}
			if len(strA) == 3 {
				modifiedInner = true
				newFn = strA[1] + strA[2]
			}
		}
		if modifiedInner {
			doWork = true
			modified = true
		}
	}
	return
}
func potentialFilename(directory, fn, extension string, i int) (string, bool) {
	pfn := fn + "(" + strconv.Itoa(i) + ")" + extension
	_, err := os.Stat(filepath.Join(directory, pfn))
	return pfn, !errors.Is(err, os.ErrNotExist)
}

// CheckRename Check the supplied structure and try and rename it
func (af AutoFix) CheckRename(fs FileStruct) (FileStruct, bool) {
	var modified bool
	var mod bool
	directory := fs.Directory()
	// Test to see if it matches one of the patterns and modify it

	fsNew := fs

	// If what we would like to call it already exists
	// Rewrite the name to be a non-conflicting (n) format
	base, extension := StripExtension(fs.Name)
	if extension == "" {
		// Do nothing for files we don't recognise
		return fs, false
	}

	fn1, mod := af.stripNumber(base)
	modified = modified || mod
	fn1, mod = af.stripRegEx(fn1)
	modified = modified || mod
	fn1, mod = af.stripAllExtension(fn1)
	modified = modified || mod
	deDuplicate := []string{".", " "}
	deSuffix := deDuplicate
	var lm bool
	fn1, lm = af.destringize(deDuplicate, deSuffix, fn1)
	modified = modified || lm

	if !modified {
		// Changed nothing, so go no further
		return fs, false
	}

	fsNew.Name = ResolveFnClash(directory, fn1, extension, fs.Name)
	if fsNew.Name != fs.Name {
		log.Println("Rename:", fs.Path(), " to ", fsNew.Path())
		if af.RenameFiles {
			if !testMode {
				err := MoveFile(fs.Path(), fsNew.Path())
				if err != nil {
					log.Println("Failed to move:", fs.Path(), "\nTo:", fsNew.Path(), "\nBecause:", err)
					return fs, false
				}
				fp := fsNew.Path()
				fss, err := os.Stat(string(fp))
				if os.IsNotExist(err) {
					log.Fatal("File we have moved to does not exist", fp)
				}
				fsNew.Mtime = fss.ModTime().Unix()
			}
			return fsNew, true
		}
	}
	return fs, false
}

func (af AutoFix) stripAllExtension(base string) (string, bool) {
	var modified bool
	for base2, ext2 := StripExtension(base); ext2 != ""; base2, ext2 = StripExtension(base) {
		base = base2
		modified = true
	}
	return base, modified
}

func (af AutoFix) destringize(deDuplicate, deSuffix []string, fn1 string) (string, bool) {
	var modified bool
	for _, dd := range deDuplicate {
		fn1, modified = af.replaceDoubles(dd, fn1, modified)
	}
	for _, ds := range deSuffix {
		fn1, modified = af.removeSuffix(ds, fn1, modified)
	}
	return fn1, modified
}

func (af AutoFix) removeSuffix(ds string, fn1 string, modified bool) (string, bool) {
	if strings.HasSuffix(fn1, ds) {
		fn1 = fn1[:len(fn1)-len(ds)]
		modified = true
	}
	return fn1, modified
}

func (af AutoFix) replaceDoubles(dd string, fn1 string, modified bool) (string, bool) {
	ddd := dd + dd
	for strings.Contains(fn1, ddd) {
		fn1 = strings.Replace(fn1, ddd, dd, -1)
		modified = true
	}
	return fn1, modified
}

// ResolveFnClash Resolve filename clashes by adding a bracketed numeral
// The numberal is incremented until we don't clash anymore
func ResolveFnClash(directory, fn string, extension, orig string) string {
	pfn := fn + extension
	if _, err := os.Stat(filepath.Join(directory, pfn)); !errors.Is(err, os.ErrNotExist) {
		exist := true
		for i := 0; exist; i++ {
			pfn, exist = potentialFilename(directory, fn, extension, i)
			if pfn == orig {
				// If we are back to our original
				// break!
				exist = false
			}
		}
	}
	return pfn
}

// WkFun Walk function across the supplied directories
// FIXME add testcases for this function
func (af *AutoFix) WkFun(dm DirectoryMap, directory, file string, d fs.DirEntry) error {
	fs, ok := dm.Get(file)
	if !ok {
		return errors.New("asked to update a file that does not exist")
	}

	err := dm.pruneEmptyFile(directory, file, fs, af.DeleteFiles)
	if err != nil {
		return err
	}

	// now look to see if we should rename the file
	fs, modified := af.CheckRename(fs)

	// Now look to see if we have seen this file's hash before
	cSum := fs.Checksum
	af.FhLock.RLock()
	oldFs, ok := af.FileHash[cSum]
	af.FhLock.RUnlock()
	if ok {
		var mod bool
		if fs.Equal(oldFs) {
			fs, mod = af.ResolveTwo(fs, oldFs)
			modified = modified || mod
		}
	}

	af.FhLock.Lock()
	af.FileHash[cSum] = fs
	af.FhLock.Unlock()
	if modified {
		dm.Add(fs)
	}
	return nil
}
