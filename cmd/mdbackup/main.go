package main

import (
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"time"

	"github.com/cbehopkins/medorg/pkg/consumers"
	"github.com/cbehopkins/medorg/pkg/core"
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
	AF         *consumers.AutoFix
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

func poolCopier(src, dst core.Fpath, pool *pb.Pool, wg *sync.WaitGroup) error {
	myBar := new(pb.ProgressBar)
	myBar.Set("prefix", fmt.Sprint(string(src), ":"))
	myBar.Set(pb.Bytes, true)
	srcSize := sizeOf(string(src))
	myBar.SetTotal(int64(srcSize))

	pool.Add(myBar)
	myBar.Start()
	defer func() {
		_ = pool.Remove(myBar)
	}()
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

	return core.CopyFile(src, dst)
}

func topRegisterFunc(dt *core.DirTracker, pool *pb.Pool, wg *sync.WaitGroup) {
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
	someVisitFunc func(dm core.DirectoryMap, dir, fn string, d fs.DirEntry, fileStruct core.FileStruct, fileInfo fs.FileInfo) error,
) {
	var wg sync.WaitGroup
	registerFunc := func(dt *core.DirTracker) {
		topRegisterFunc(dt, pool, &wg)
	}
	errChan := core.VisitFilesInDirectories(directories, registerFunc, someVisitFunc)
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
	visitFunc := func(dm core.DirectoryMap, dir, fn string, d fs.DirEntry, fileStruct core.FileStruct, fileInfo fs.FileInfo) error {
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
	var xc *core.XMLCfg
	if xmcf := core.XmConfig(); xmcf != "" {
		// FIXME should we be casting to string here or fixing the interfaces?
		var err error
		xc, err = core.NewXMLCfg(string(xmcf))
		if err != nil {
			fmt.Println("Error loading config file:", err)
			retcode = ExitNoConfig
			return
		}
	} else {
		fmt.Println("no config file found")
		fn := filepath.Join(string(core.HomeDir()), "/.core.xml")
		xc, err = core.NewXMLCfg(fn)
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
		// If no directories provided, read from config
		// First directory should be destination (current directory)
		directories = []string{"."}

		// Add source directories from config
		if xc != nil {
			sourcePaths := xc.GetSourcePaths()
			if len(sourcePaths) > 0 {
				directories = append(directories, sourcePaths...)
			}
		}
	}

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
				fmt.Println("Ctrl-C Detected")
				close(shutdownChan)
			} else {
				os.Exit(1)
			}
		}
	}()

	///////////////////////////////////
	// Create config and run
	var dest string
	var sources []string
	if len(directories) > 0 {
		dest = directories[0]
		if len(directories) > 1 {
			sources = directories[1:]
		}
	}
	cfg := Config{
		Destination:          dest,
		Sources:              sources,
		VolumeConfigProvider: xc,
		TagMode:              *tagflg,
		ScanMode:             *scanflg,
		DummyMode:            *dummyflg,
		DeleteMode:           *delflg,
		StatsMode:            *statsflg,
		LogOutput:            f,
		MessageWriter:        os.Stdout,
		ShutdownChan:         shutdownChan,
		UseProgressBar:       true,
	}

	retcode, err = Run(cfg)
	if err != nil {
		fmt.Println("Error:", err)
	}
}
