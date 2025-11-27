package main

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"time"

	"github.com/cbehopkins/medorg"
	pb "github.com/cbehopkins/pb/v3"
	bytesize "github.com/inhies/go-bytesize"
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

// FIXME
var (
	MaxBackups = 2
	AF         *medorg.AutoFix
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

func poolCopier(src, dst medorg.Fpath, pool *pb.Pool, wg *sync.WaitGroup) error {
	myBar := new(pb.ProgressBar)
	myBar.Set("prefix", fmt.Sprint(string(src), ":"))
	myBar.Set(pb.Bytes, true)
	srcSize := sizeOf(string(src))
	myBar.SetTotal(int64(srcSize))

	pool.Add(myBar)
	myBar.Start()
	defer pool.Remove(myBar)
	closeChan := make(chan struct{})
	defer func() { close(closeChan) }()
	wg.Add(1)
	go func() {
		for {
			select {
			case <-time.After(2 * time.Second):
				dstSize := sizeOf(string(dst))
				myBar.SetCurrent(int64(dstSize))
			case <-closeChan:
				myBar.Finish()
				wg.Done()
				return
			}
		}
	}()

	return medorg.CopyFile(src, dst)
}

func topRegisterFunc(dt *medorg.DirTracker, pool *pb.Pool, wg *sync.WaitGroup) {
	removeFunc := func(pb *pb.ProgressBar) {
		err := pool.Remove(pb)
		if err != nil {
			log.Println("Failed to remove bar::", err)
		}
		wg.Done()
	}

	bar := pb.RegisterProgressable(dt, removeFunc)
	pool.Add(bar)
	wg.Add(1)
}

func visitFilesUpdatingProgressBar(pool *pb.Pool, directories []string,
	someVisitFunc func(dm medorg.DirectoryMap, dir, fn string, d fs.DirEntry, fileStruct medorg.FileStruct, fileInfo fs.FileInfo) error,
) {
	var wg sync.WaitGroup
	registerFunc := func(dt *medorg.DirTracker) {
		topRegisterFunc(dt, pool, &wg)
	}
	errChan := medorg.VisitFilesInDirectories(directories, registerFunc, someVisitFunc)
	for err := range errChan {
		log.Println("Error Got...", err)
	}
	wg.Wait()
}

func runStats(pool *pb.Pool, messageBar *pb.ProgressBar, directories []string) {
	messageBar.Set("msg", "Start Scanning")
	var lk sync.Mutex
	// I want to know the size of storage I need to buy to get the files backed
	// up n times
	// So for each backup count, I want to know the size of the files
	// i.e. how many bytes are backed up 0 times
	// How many bytes are backed up 1 time
	totalArray := make([]int64, MaxBackups+1)
	for i := range totalArray {
		totalArray[i] = 0
	}
	visitFunc := func(dm medorg.DirectoryMap, dir, fn string, d fs.DirEntry, fileStruct medorg.FileStruct, fileInfo fs.FileInfo) error {
		lenArchive := len(fileStruct.BackupDest)
		lenNeedesAdding := (lenArchive + 1) - len(totalArray)

		if lenNeedesAdding > 0 {
			lk.Lock()
			totalArray = append(totalArray, make([]int64, lenNeedesAdding)...)
			lk.Unlock()
		}
		fileSize := fileInfo.Size()

		lk.Lock()
		// Would like to do this with atomic add. The need to resize array prevents this
		totalArray[lenArchive] += int64(fileSize)
		lk.Unlock()
		return nil
	}
	visitFilesUpdatingProgressBar(pool, directories, visitFunc)

	for i, val := range totalArray {
		// WTF why would you have a fraction number of bytes????
		b := bytesize.New(float64(val))
		log.Println(i, "requires", b, "bytes")
	}
}

var LOGFILENAME = "mdbackup.log"

func main() {
	retcode := 0
	defer func() { os.Exit(retcode) }()

	///////////////////////////////////
	// Logging setup
	os.Remove(LOGFILENAME)
	f, err := os.OpenFile(LOGFILENAME, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o666)
	if err != nil {
		fmt.Printf("error opening log file: %v\n", err)
		retcode = 1
		return
	}
	defer f.Close()

	log.SetOutput(f)
	log.Println("This is a test log entry")

	var directories []string
	///////////////////////////////////
	// Read in top level config
	var xc *medorg.XMLCfg
	if xmcf := medorg.XmConfig(); xmcf != "" {
		// FIXME should we be casting to string here or fixing the interfaces?
		var err error
		xc, err = medorg.NewXMLCfg(string(xmcf))
		if err != nil {
			fmt.Println("Error loading config file:", err)
			retcode = ExitNoConfig
			return
		}
	} else {
		fmt.Println("no config file found")
		fn := filepath.Join(string(medorg.HomeDir()), "/.medorg.xml")
		xc, err = medorg.NewXMLCfg(fn)
		if err != nil {
			fmt.Println("Error creating config file:", err)
			retcode = ExitNoConfig
			return
		}
	}
	if xc == nil {
		fmt.Println("Unable to get config")
		retcode = ExitNoConfig
		return
	}
	defer func() {
		fmt.Println("Saving out config")
		err := xc.WriteXmlCfg()
		if err != nil {
			fmt.Println("Error while saving config file", err)
		}
	}()

	///////////////////////////////////
	// Command line argument processing
	tagflg := flag.Bool("tag", false, "Locate and print the directory tag, create if needed")
	scanflg := flag.Bool("scan", false, "Only scan files in src & dst updating labels, don't run the backup")
	dummyflg := flag.Bool("dummy", false, "Don't copy, just tell me what you'd do")
	delflg := flag.Bool("delete", false, "Delete duplicated Files")
	statsflg := flag.Bool("stats", false, "Generate backup statistics")

	flag.Parse()
	if flag.NArg() > 0 {
		for _, fl := range flag.Args() {
			_, err := os.Stat(fl)
			if os.IsNotExist(err) {
				fmt.Println(fl, "does not exist!")
				retcode = ExitSuppliedDirNotFound
				return
			}
			if isDir(fl) {
				directories = append(directories, fl)
			}
		}
	} else {
		directories = []string{"."}
	}

	///////////////////////////////////
	// Progress Bar init
	messageBar := new(pb.ProgressBar)
	pool := pb.NewPool(messageBar)
	err = pool.Start()
	if err != nil {
		fmt.Println("Err::", err)
		retcode = ExitBadVc
		return
	}

	defer pool.Stop()
	defer messageBar.Finish()
	messageBar.SetTemplateString(`{{string . "msg"}}`)
	messageBar.Set("msg", "Initialzing discombobulator")

	///////////////////////////////////
	// Catch Ctrl-C sensibly!
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	shutdownChan := make(chan struct{})
	go func() {
		ccCnt := 0
		for range signalChan {
			ccCnt++
			if ccCnt == 1 {
				messageBar.Set("msg", "Ctrl-C Detected")
				close(shutdownChan)
			} else {
				os.Exit(1)
			}
		}
	}()

	logBar := new(pb.ProgressBar)
	defer logBar.Finish()
	logBar.SetTemplateString(`{{string . "msg"}}`)
	pool.Add(logBar)

	///////////////////////////////////
	// Support tasks to main backup function need to run first
	if *tagflg {
		if len(directories) != 1 {
			fmt.Println("One directory only please when configuring tags")
			retcode = ExitOneDirectoryOnly
			return
		}
		vc, err := xc.VolumeCfgFromDir(directories[0])
		if err != nil {
			fmt.Println("Err::", err)
			retcode = ExitBadVc
			return
		}
		fmt.Println("Config name is", vc.Label)
		return
	}

	if *statsflg {
		runStats(pool, messageBar, directories)
		return
	}

	///////////////////////////////////
	// Main backup code starts
	///////////////////////////////////
	if len(directories) != 2 {
		fmt.Println("Error, expected 2 directories!", directories)
		retcode = ExitTwoDirectoriesOnly
		return
	}

	// Setup the function that copies files
	var wg sync.WaitGroup
	var copyer func(src, dst medorg.Fpath) error
	if *dummyflg {
		copyer = func(src, dst medorg.Fpath) error {
			log.Println("Copy from:", src, " to ", dst)
			return medorg.ErrDummyCopy
		}
	} else {
		copyer = func(src, dst medorg.Fpath) error {
			return poolCopier(src, dst, pool, &wg)
		}
	}
	if *scanflg {
		copyer = nil
	}

	// Setup the function that deals with orphaned files
	// i.e. files that are on the backup, but not the source
	var orphanedFunc func(string) error
	if *dummyflg {
		orphanedFunc = func(path string) error {
			log.Println(path, "orphaned")
			return nil
		}
	} else if *delflg {
		orphanedFunc = func(path string) error {
			log.Println(path, "orphaned")
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				_ = os.Remove(path)
			}
			return nil
		}
	}

	logFunc := func(msg string) {
		logBar.Set("msg", msg)
		log.Println(msg)
	}

	// This little bit of code means we get a progress bar on
	// the backup runner scanning through directory trees
	// This can take quite some time as it has to load and possibly generate
	// the xml descriptions for the files (md5 hash calculation)
	registerFunc := func(dt *medorg.DirTracker) {
		topRegisterFunc(dt, pool, &wg)
	}

	messageBar.Set("msg", "Starting Backup Run")
	err = medorg.BackupRunner(xc, 2, copyer, directories[0], directories[1], orphanedFunc, logFunc, registerFunc, shutdownChan)
	messageBar.Set("msg", "Completed Backup Run")

	if err != nil {
		messageBar.Set("msg", fmt.Sprint("Unable to complete backup:", err))
		retcode = ExitIncompleteBackup
		return
	}
	messageBar.Set("msg", "Waiting for complete")
	wg.Wait()
}
