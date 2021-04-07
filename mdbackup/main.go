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
		// FIXME should we be casting to string here or fixing then interfaces?
		xc = medorg.NewXMLCfg(string(xmcf))
		AF = medorg.NewAutoFix(xc.Af)
	} else {
		fmt.Println("no config file found")
		fn := filepath.Join(string(medorg.HomeDir()), "/.medorg.xml")
		xc = medorg.NewXMLCfg(fn)
		AF = medorg.NewAutoFix([]string{})
	}
	AF.SilenceLogging = true
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

	///////////////////////////////////
	// Pass 1, go through and make sure everything is up to date
	for _, directory := range directories {
		tw := medorg.NewTreeWalker()
		tw.WalkTree(directory, AF.WkFun, nil)
	}
	if len(directories) != 2 {
		fmt.Println("Error, expected 2 directories!")
		os.Exit(ExitTwoDirectoriesOnly)
	}

	///////////////////////////////////
	// Pass 2, do the backup
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
		err := medorg.CopyFile(src, dst)
		close(closeChan)
		return err
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
