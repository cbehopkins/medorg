package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/cbehopkins/medorg"
)

var AF *medorg.AutoFix

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
	var walkCnt = flag.Int("walk", 2, "Max Number of directory Walkers")
	var calcCnt = flag.Int("calc", 2, "Max Number of MD5 calculators")
	var delflg = flag.Bool("delete", false, "Delete duplicated Files")
	var rnmflg = flag.Bool("rename", false, "Auto Rename Files")
	var skpflg = flag.Bool("skipu", false, "Skip update phase - go stright to autofix")
	var bldflg = flag.Bool("buildo", false, "Build Only - do not run autofix")
	var autflg = flag.Bool("auto", false, "Autofix filenames: -rename and -delete turn this on")
	var conflg = flag.Bool("conc", false, "Concentrate files together in same directory")
	var mvdflg = flag.Bool("mvd", false, "Move Detect - look for same name and size in a different directory")
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
	pendCnt := *calcCnt + *walkCnt
	if *mvdflg {
		tw := medorg.NewTreeUpdate(*walkCnt, *calcCnt, pendCnt)
		tw.MoveDetect(directories)
	}
	// Subtle - we want the walk engine to be able to start a calc routing
	// without that calc routine having a token as yet
	// i.e. we want the go scheduler to have some things queued up to do
	// This allows us to set calcCnt to the amount of IO we want
	// and walkCnt to be set to allow the directory structs to be hammered
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
			tw.WalkTree(directory, AF.WkFun, nil)
		}
	}
	if *conflg {
		for _, directory := range directories {
			//wf := func(srcDir, fn string, fs medorg.FileStruct, dm *medorg.DirectoryMap) bool {
			//	return AF.Consolidate(srcDir, fn, directory)
			//}
			df := func(dir string, dm *medorg.DirectoryMap) {
				var moved bool
				fc := func(fn string, fs medorg.FileStruct) {
					var mov bool
					mov = AF.Consolidate(dir, fn, directory)
					moved = moved || mov
				}
				dm.Range(fc)
				return
			}
			tw := medorg.NewTreeWalker()
			tw.WalkTree(directory, nil, df)
		}
	}
}
