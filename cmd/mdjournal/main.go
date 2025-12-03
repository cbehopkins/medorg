package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/cbehopkins/medorg/pkg/core"
)

const (
	ExitOk = iota
	ExitSuppliedDirNotFound
	ExitWalkError
	ExitJournalWriteError
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
	scanflg := flag.Bool("scan", false, "Only scan files in src & dst updating labels, don't run the backup")

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

	cfg := Config{
		Directories:  directories,
		JournalPath:  string(core.ConfigPath(".mdjournal.xml")),
		ScanOnly:     *scanflg,
		ReadExisting: true,
	}

	exitCode, err := Run(cfg)
	if err != nil {
		fmt.Println(err)
	}
	os.Exit(exitCode)
}
