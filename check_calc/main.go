package main

import (
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/cbehopkins/medorg"
)

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

	var calcCnt = flag.Int("calc", 2, "Max Number of MD5 calculators")
	var delflg = flag.Bool("delete", false, "Delete duplicated Files")
	var mvdflg = flag.Bool("mvd", false, "Move Detect")
	var rnmflg = flag.Bool("rename", false, "Auto Rename Files")
	var rclflg = flag.Bool("recalc", false, "Recalculate all checksums")

	var conflg = flag.Bool("conc", false, "Concentrate files together in same directory")
	//var mvdflg = flag.Bool("mvd", false, "Move Detect - look for same name and size in a different directory")
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

	var AF *medorg.AutoFix
	if *rnmflg {
		var xc *medorg.XMLCfg
		if xmcf := medorg.XmConfig(); xmcf != "" {
			// FIXME should we be casting to string here or fixing the interfaces?
			xc = medorg.NewXMLCfg(string(xmcf))
		} else {
			fmt.Println("no config file found")
			fn := filepath.Join(string(medorg.HomeDir()), medorg.Md5FileName)
			xc = medorg.NewXMLCfg(fn)
		}
		AF = medorg.NewAutoFix(xc.Af)
		AF.DeleteFiles = *delflg
	}

	if *mvdflg {
		err := medorg.NewMoveDetect().RunMoveDetect(directories)
		if err != nil {
			fmt.Println("Error! In move detect", err)
			os.Exit(4)
		}
		fmt.Println("Finished move detection")
	}


	var con *medorg.Concentrator

	// Have a buffer of compute tokens
	// to ensure we're not doing too much at once
	tokenBuffer := make(chan struct{}, *calcCnt)
	defer close(tokenBuffer)
	for i := 0; i < *calcCnt; i++ {
		tokenBuffer <- struct{}{}
	}

	visitor := func(dm medorg.DirectoryMap, directory, file string, d fs.DirEntry) error {
		if file == medorg.Md5FileName {
			return nil
		}
		err := dm.UpdateValues(directory, d)
		if err != nil {
			return err
		}
		// Grab a compute token
		<-tokenBuffer
		err = dm.UpdateChecksum(directory, file, *rclflg)
		tokenBuffer <- struct{}{}

		if AF != nil {
			AF.WkFun(dm, directory, file, d)
		}
		if con != nil {
			con.Visiter(dm, directory, file, d)
		}
		return err
	}

	makerFunc := func(dir string) (medorg.DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (medorg.DirectoryEntryInterface, error) {
			dm, err := medorg.DirectoryMapFromDir(dir)
			if err != nil {
				return dm, err
			}
			dm.VisitFunc = visitor
			if con != nil {
				err := con.DirectoryVisit(dm, dir)
				if err != nil {
					fmt.Println("Received error from concentrate", err)
					os.Exit(3)
				}
			}
			return dm, dm.DeleteMissingFiles()
		}
		de := medorg.NewDirectoryEntry(dir, mkFk)
		return de, nil
	}
	for _, dir := range directories {
		if *conflg {
			con = medorg.NewConcentrator(dir)
		}
		errChan := medorg.NewDirTracker(dir, makerFunc)

		for err := range errChan {
			fmt.Println("Error received while walking:", dir, err)
			os.Exit(2)
		}
	}
	fmt.Println("Finished walking")
}
