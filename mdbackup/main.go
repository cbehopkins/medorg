package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cbehopkins/medorg"
	"github.com/cheggaaa/pb/v3"
)

const (
	ExitOk = iota
	ExitNoConfig
	ExitTwoDirectoriesOnly
	ExitProgressBar
	ExitIncompleteBackup
	ExitSuppliedDirNotFound
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

func sizeOf(fn string) int {
	fi, err := os.Stat(fn)
	if err != nil {
		return 0
	}
	fs := fi.Size() / 1024
	if fs > (1 << 31) {
		return (1 << 31) - 1
	}
	return int(fs)
}

func main() {
	var directories []string
	var xc *medorg.XMLCfg
	if xmcf := medorg.XmConfig(); xmcf != "" {
		// FIXME should we be casting to string here or fixing the interfaces?
		xc = medorg.NewXMLCfg(string(xmcf))
	} else {
		fmt.Println("no config file found")
		fn := filepath.Join(string(medorg.HomeDir()), "/.medorg.xml")
		xc = medorg.NewXMLCfg(fn)
	}
	defer func() {
		fmt.Println("Saving out config")
		err := xc.WriteXmlCfg()
		if err != nil {
			fmt.Println("Error while saving config file", err)
		}
	}()
	var scanflg = flag.Bool("scan", false, "Only scan files, don't run the backup")
	var dummyflg = flag.Bool("dummy", false, "Don't copy, just tell me what you'd do")

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

	if len(directories) != 2 {
		fmt.Println("Error, expected 2 directories!", directories)
		os.Exit(ExitTwoDirectoriesOnly)
	}

	var lk sync.Mutex
	bar := pb.New(0)

	defer func() {
		if bar.IsStarted() {
			lk.Lock()
			bar.Finish()
			lk.Unlock()
		}
	}()
	var wg sync.WaitGroup
	copyer := func(src, dst medorg.Fpath) error {
		srcSize := sizeOf(string(src))

		closeChan := make(chan struct{})
		wg.Add(1)
		go func() {
			lk.Lock()
			defer lk.Unlock()
			defer wg.Done()
			if !bar.IsStarted() {
				bar.Start()
			}

			bar.SetTotal(int64(srcSize))
			for {
				select {
				case <-time.After(2 * time.Second):
					dstSize := sizeOf(string(dst))
					bar.SetCurrent(int64(dstSize))
				case <-closeChan:
					return
				}
			}
		}()
		if *dummyflg {
			fmt.Println("Copy from:", src, " to ", dst)
			return nil
		}
		err := medorg.CopyFile(src, dst)
		close(closeChan)
		return err
	}
	if *scanflg {
		copyer = nil
	}
	fmt.Println("Starting Backup Run")
	err := medorg.BackupRunner(xc, copyer, directories[0], directories[1])
	fmt.Println("Completed Backup Run")

	if err != nil {
		fmt.Println("Unable to complete backup:", err)
		os.Exit(ExitIncompleteBackup)
	}
	wg.Wait()
	fmt.Println("Completed Copying")
}
