package main

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"

	"github.com/cbehopkins/medorg"
)

const (
	ExitOk = iota
	ExitSuppliedDirNotFound
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
	var scanflg = flag.Bool("scan", false, "Only scan files in src & dst updating labels, don't run the backup")

	flag.Parse()
	if flag.NArg() > 0 {
		for _, fl := range flag.Args() {
			_, err := os.Stat(fl)
			if os.IsNotExist(err) {
				fmt.Println(fl, "does not exist!")
				os.Exit(ExitSuppliedDirNotFound)
			}
			if isDir(fl) {
				directories = append(directories, fl)
			}
		}
	} else {
		directories = []string{"."}
	}

	if *scanflg {
		fmt.Println("You've asked us to scan:", directories)
	}

	journal := medorg.Journal{}

	visitor := func(dm medorg.DirectoryMap, directory, file string, d fs.DirEntry) error {
		return nil
	}

	makerFunc := func(dir string) (medorg.DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (medorg.DirectoryEntryInterface, error) {
			dm, err := medorg.DirectoryMapFromDir(dir)
			if err != nil {
				return dm, err
			}
			dm.VisitFunc = visitor

			return dm, journal.AppendJournalFromDm(&dm, dir)
		}
		de, err := medorg.NewDirectoryEntry(dir, mkFk)
		return de, err
	}
	fn := string(medorg.ConfigPath(".mdjournal.xml"))
	fh, err := os.Open(fn)
	if !errors.Is(err, os.ErrNotExist) {
		fmt.Println("Reading in journal")
		journal.FromReader(fh)
		err := fh.Close()
		if err != nil {
			fmt.Println("Error closing read in journal:", err)
		}
	}

	fh, err = os.Create(fn)
	if err != nil {
		fmt.Println("Unable to open journal for writing:", err, "::", fn)
		os.Exit(3)
	}
	defer fh.Close()
	for _, dir := range directories {
		errChan := medorg.NewDirTracker(dir, makerFunc).Start().ErrChan()
		for err := range errChan {
			fmt.Println("Error received while walking:", dir, err)
			os.Exit(2)
		}
	}

	err = journal.ToWriter(fh)
	if err != nil {
		fmt.Println("Error writing Journal:", err)
		os.Exit(3)
	}
}
