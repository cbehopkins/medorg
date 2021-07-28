package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cbehopkins/medorg"
	pb "github.com/cbehopkins/pb/v3"
)

const (
	ExitOk = iota
	ExitNoConfig
	ExitOneDirectoryOnly
	ExitTwoDirectoriesOnly
	ExitProgressBar
	ExitIncompleteBackup
	ExitSuppliedDirNotFound
	ExitBadVc
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
	if xc == nil {
		fmt.Println("Unable to get config")
		os.Exit(ExitNoConfig)
	}
	defer func() {
		fmt.Println("Saving out config")
		err := xc.WriteXmlCfg()
		if err != nil {
			fmt.Println("Error while saving config file", err)
		}
	}()
	var tagflg = flag.Bool("tag", false, "Locate and print the directory tag, create if needed")

	var scanflg = flag.Bool("scan", false, "Only scan files in src & dst updating labels, don't run the backup")
	var dummyflg = flag.Bool("dummy", false, "Don't copy, just tell me what you'd do")
	var delflg = flag.Bool("delete", false, "Delete duplicated Files")

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

	messageBar := new(pb.ProgressBar)
	pool := pb.NewPool(messageBar)
	err := pool.Start()
	if err != nil {
		fmt.Println("Err::", err)
		os.Exit(ExitBadVc)
	}

	defer pool.Stop()
	defer messageBar.Finish()
	messageBar.SetTemplateString(`{{string . "backup status"}}`)
	messageBar.Set("backup status", "Initialzing discombobulator")

	if *tagflg {
		if len(directories) != 1 {
			fmt.Println("One directory only please when configuring tags")
			os.Exit(ExitOneDirectoryOnly)
		}
		vc, err := xc.VolumeCfgFromDir(directories[0])
		if err != nil {
			fmt.Println("Err::", err)
			os.Exit(ExitBadVc)
		}
		fmt.Println("Config name is", vc.Label)
		os.Exit(ExitOk)
	}

	if len(directories) != 2 {
		fmt.Println("Error, expected 2 directories!", directories)
		os.Exit(ExitTwoDirectoriesOnly)
	}

	var wg sync.WaitGroup
	copyer := func(src, dst medorg.Fpath) error {
		if *dummyflg {
			fmt.Println("Copy from:", src, " to ", dst)
			return medorg.ErrDummyCopy
		}
		myBar := new(pb.ProgressBar)
		myBar.Set("prefix", fmt.Sprint(string(src), ":"))
		myBar.Set(pb.Bytes, true)
		srcSize := sizeOf(string(src))
		myBar.SetTotal(int64(srcSize))

		pool.Add(myBar)
		myBar.Start()
		defer pool.Remove(myBar)
		closeChan := make(chan struct{})
		wg.Add(1)
		go func() {
			for {
				select {
				case <-time.After(2 * time.Second):
					dstSize := sizeOf(string(dst))
					myBar.SetCurrent(int64(dstSize))
				case <-closeChan:
					myBar.Finish()
					return
				}
			}
		}()

		err := medorg.CopyFile(src, dst)
		close(closeChan)
		return err
	}
	var orphanedFunc func(string) error
	if *scanflg {
		copyer = nil
	}
	if *dummyflg {
		orphanedFunc = func(path string) error {
			fmt.Println(path, "orphaned")
			return nil
		}
	} else if *delflg {
		orphanedFunc = func(path string) error {
			fmt.Println(path, "orphaned")
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				_ = os.Remove(path)
			}
			return nil
		}
	}
	messageBar.Set("backup status", "Starting Backup Run")
	logBar := new(pb.ProgressBar)
	defer logBar.Finish()
	logBar.SetTemplateString(`{{string . "msg"}}`)
	pool.Add(logBar)
	dirBar := new(pb.ProgressBar)
	defer dirBar.Finish()
	dirBar.SetTemplateString(`{{string . "msg"}}`)
	pool.Add(dirBar)
	blanker := func(msg string, bar *pb.ProgressBar) string {
		toAdd := bar.Width() - len(msg)
		if toAdd < 0 {
			toAdd = 0
		}
		return fmt.Sprint(msg, strings.Repeat(" ", toAdd))
	}
	logFunc := func(msg string) {
		logBar.Set("msg", blanker(msg, logBar))
	}
	traverseFunc := func(path string) {
		dirBar.Set("msg", blanker(path, dirBar))
	}
	err = medorg.BackupRunner(xc, copyer, directories[0], directories[1], orphanedFunc, logFunc, traverseFunc)
	messageBar.Set("backup status", "Completed Backup Run")

	if err != nil {
		messageBar.Set("backup status", fmt.Sprint("Unable to complete backup:", err))
		os.Exit(ExitIncompleteBackup)
	}
	wg.Wait()
	messageBar.Set("backup status", "Completed Copying")
}
