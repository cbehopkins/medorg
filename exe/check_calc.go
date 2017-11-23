package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/cbehopkins/medorg"
)

var FileHash map[string]medorg.FileStruct
var AF *medorg.AutoFix

func wkFun(directory, fn string, fs medorg.FileStruct, dm *medorg.DirectoryMap) bool {
	var modified bool
	if fs.Directory() != directory {
		log.Fatal("Structure Problem for", directory, fn)
	}
	if fs.Size == 0 {
		fmt.Println("Zero Length File")
		if AF.DeleteFiles {
			err := dm.RmFile(directory, fn)
			if err != nil {
				log.Fatal("Couldn't delete file", directory, fn)
			}
		}
		return true
	}
	// now look to see if we should rename the file
	var mod bool
	fs, mod = AF.CheckRename(fs)
	modified = modified || mod

	// Check if two of the checksums are equal
	cSum := fs.Checksum
	oldFs, ok := FileHash[cSum]
	if ok {
		fs, mod = AF.ResolveTwo(fs, oldFs)
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
	// TBD Populate this from a file
  var DomainList = []string{"(.*)_calc"}
  AF = medorg.NewAutoFix(DomainList)
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
