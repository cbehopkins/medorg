package main

import (
	"flag"
	"fmt"
	"io/fs"
	"os"
	"strings"

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
	//var delflg = flag.Bool("delete", false, "Delete duplicated Files")
	//var rnmflg = flag.Bool("rename", false, "Auto Rename Files")
	var rclflg = flag.Bool("recalc", false, "Recalculate all checksums")

	//var conflg = flag.Bool("conc", false, "Concentrate files together in same directory")
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

	tokenBuffer := make(chan struct{}, *calcCnt)
	defer close(tokenBuffer)
	for i := 0; i < *calcCnt; i++ {
		tokenBuffer <- struct{}{}
	}

	visitor := func(de medorg.DirectoryEntry, directory, file string, d fs.DirEntry) error {
		if strings.HasPrefix(file, ".") {
			// Skip hidden files
			return nil
		}
		err := de.UpdateValues(d)
		if err != nil {
			return err
		}
		<-tokenBuffer
		err = de.UpdateChecksum(file, *rclflg)
		tokenBuffer <- struct{}{}
		if err != nil {
			return err
		}
		return nil
	}

	makerFunc := func(dir string) medorg.DirectoryTrackerInterface {
		return medorg.NewDirectoryEntry(dir, visitor)
	}
	for _, dir := range directories {
		errChan := medorg.NewDirTracker(dir, makerFunc)

		for err := range errChan {
			fmt.Println("Error received on closing:", err)
			os.Exit(2)
		}
	}
	fmt.Println("Finished walking")
}
