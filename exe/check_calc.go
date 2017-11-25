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
		if fs.Size == oldFs.Size {
			fs, mod = AF.ResolveTwo(fs, oldFs)
			modified = modified || mod
		}
	}

	FileHash[cSum] = fs
	if modified {
		//fmt.Println("Modified FS:", fs)
		dm.Rm(fn)
		dm.Add(fs)
	}
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
	if xmcf := medorg.XmConfig(); xmcf != "" {
		xc := medorg.NewXMLCfg(xmcf)
		for _, v := range xc.Af {
			fmt.Printf("Add AutoFix Rule:%q\n", v)
		}
		AF = medorg.NewAutoFix(xc.Af)
	} else if afcf := medorg.AfConfig(); afcf != "" {
		AF = medorg.NewAutoFixFile(afcf)
	} else {
		var DomainList = []string{"(.*)_calc"}
		AF = medorg.NewAutoFix(DomainList)
	}
	FileHash = make(map[string]medorg.FileStruct)
	var walkCnt = flag.Int("walk", 2, "Max Number of directory Walkers")
	var calcCnt = flag.Int("calc", 2, "Max Number of MD5 calculators")
	var delflg = flag.Bool("delete", false, "Delete duplicated Files")
	var rnmflg = flag.Bool("rename", false, "Auto Rename Files")
	var skpflg = flag.Bool("skipu", false, "Skip update phase - go stright to autofix")
	var bldflg = flag.Bool("buildo", false, "Build Only - do not run autofix")
	var autflg = flag.Bool("auto", false, "Autofix filenames: -rename and -delete turn this on")
	flag.Parse()
	AF.DeleteFiles = *delflg
	AF.RenameFiles = *rnmflg

	if flag.NArg() > 0 {
		for _, fl := range flag.Args() {
			if isDir(fl) {
				directories = append(directories, fl)
			}
		}
	} else {
		directories = []string{"."}
	}

	if *delflg || *rnmflg {
		*autflg = !*bldflg
	}
	// Subtle - we want the walk engine to be able to start a calc routing
	// without that calc routine having a token as yet
	// i.e. we want the go scheduler to have some things queued up to do
	// This allows us to set calcCnt to the amount of IO we want
	// and walkCnt to be set to allow the directory structs to be hammered
	pendCnt := *calcCnt + *walkCnt
	for _, directory := range directories {
		if !*skpflg {
			tu := medorg.NewTreeUpdate(*walkCnt, *calcCnt, pendCnt)
			tu.UpdateDirectory(directory, nil)
		}
		if *autflg {
			tw := medorg.NewTreeWalker()
			if !*skpflg {
				tw.SetBuildComplete()
			}
			tw.WalkTree(directory, wkFun, nil)
		}
	}
}
