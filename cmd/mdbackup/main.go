package main

import (
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/cbehopkins/medorg/pkg/cli"
	"github.com/cbehopkins/medorg/pkg/consumers"
	"github.com/cbehopkins/medorg/pkg/core"
	pb "github.com/cbehopkins/pb/v3"
	bytesize "github.com/inhies/go-bytesize"
)

var (
	MaxBackups = 2
	AF         *consumers.AutoFix
)

// logMemoryStats logs current memory statistics
func logMemoryStats(prefix string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Printf("%s Memory: Alloc=%v MB, HeapInuse=%v MB, Sys=%v MB, TotalAlloc=%v MB, NumGC=%v, HeapObjects=%v, Goroutines=%v",
		prefix,
		m.Alloc/1024/1024,
		m.HeapInuse/1024/1024,
		m.TotalAlloc/1024/1024,
		m.Sys/1024/1024,
		m.NumGC,
		m.HeapObjects,
		runtime.NumGoroutine())
}

// startMemoryMonitor starts periodic memory monitoring
func startMemoryMonitor(interval time.Duration, done <-chan struct{}) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				logMemoryStats("[MONITOR]")
			case <-done:
				return
			}
		}
	}()
}

// pprofAddFlags and pprofInit are provided by build-tagged files

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

// sizeOfBytes returns file size in bytes or 0
func sizeOfBytes(fn string) int64 {
	fi, err := os.Stat(fn)
	if err != nil {
		return 0
	}
	return fi.Size()
}

// poolCopier copies a file while updating a progress bar using bytes written
func poolCopier(src, dst core.Fpath, pool *pb.Pool, wg *sync.WaitGroup) error {
	myBar := new(pb.ProgressBar)
	myBar.Set("prefix", fmt.Sprint(string(src), ":"))
	myBar.Set(pb.Bytes, true)
	total := sizeOfBytes(string(src))
	myBar.SetTotal(total)

	pool.Add(myBar)
	myBar.Start()

	// Replicate core.CopyFile with progress-enabled copy path
	srcs := string(src)
	dsts := string(dst)
	sfi, err := os.Stat(srcs)
	if err != nil {
		myBar.Finish()
		return fmt.Errorf("error in CopyFile src file status %w %s", err, srcs)
	}
	if !sfi.Mode().IsRegular() {
		myBar.Finish()
		return fmt.Errorf("CopyFile: non-regular source file %s (%q)", sfi.Name(), sfi.Mode().String())
	}
	if dfi, err := os.Stat(dsts); err == nil {
		if !(dfi.Mode().IsRegular()) {
			myBar.Finish()
			return fmt.Errorf("CopyFile: non-regular destination file %s (%q)", dfi.Name(), dfi.Mode().String())
		}
		if os.SameFile(sfi, dfi) {
			myBar.Finish()
			return nil
		}
	} else if !os.IsNotExist(err) {
		myBar.Finish()
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dsts), 0o777); err != nil {
		myBar.Finish()
		return fmt.Errorf("issue in CopyFile creating directory tree %w", err)
	}
	if err := os.Link(srcs, dsts); err == nil {
		myBar.SetCurrent(total)
		myBar.Finish()
		return nil
	}

	in, err := os.Open(srcs)
	if err != nil {
		myBar.Finish()
		return fmt.Errorf("info error on src in copyFileContents : %w", err)
	}
	defer func() { _ = in.Close() }()
	out, err := os.Create(dsts)
	if err != nil {
		myBar.Finish()
		return fmt.Errorf("unable to write to output file in copyFileContents %w %s", err, dsts)
	}
	// Ensure we close and report any close error
	var copyErr error
	defer func() {
		cerr := out.Close()
		if copyErr == nil {
			copyErr = cerr
		}
	}()

	// Stream copy with progress updates
	buf := make([]byte, 1<<20) // 1 MiB buffer
	var written int64
	for {
		nr, er := in.Read(buf)
		if nr > 0 {
			nw, ew := out.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
				myBar.SetCurrent(written)
			}
			if ew != nil {
				copyErr = ew
				break
			}
			if nr != nw {
				copyErr = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				copyErr = er
			}
			break
		}
	}
	if copyErr != nil {
		myBar.Finish()
		return copyErr
	}
	if err := out.Sync(); err != nil {
		myBar.Finish()
		return err
	}
	myBar.SetCurrent(total)
	myBar.Finish()
	return nil
}

func visitFilesUpdatingProgressBar(pool *pb.Pool, directories []string,
	someVisitFunc func(dm core.DirectoryMap, dir, fn string, d fs.DirEntry, fileStruct core.FileStruct, fileInfo fs.FileInfo) error,
) {
	factory := pb.NewPoolProgressFactory(pool)
	errChan := core.VisitFilesInDirectories(directories, factory, someVisitFunc)
	for err := range errChan {
		log.Println("Error Got...", err)
	}
	factory.Wg.Wait()
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
	var xc *core.MdConfig

	configPath := flag.String("config", "", "Path to config file (optional, defaults to ~/.mdcfg.xml)")
	// FIXME add help flag
	scanflg := flag.Bool("scan", false, "Only scan files in src & dst updating labels, don't run the backup")
	dummyflg := flag.Bool("dummy", false, "Don't copy, just tell me what you'd do")
	delflg := flag.Bool("delete", false, "Delete duplicated/orphaned Files")
	statsflg := flag.Bool("stats", false, "Generate backup statistics")
	skipCheckCalcFlg := flag.Bool("skip-checkcalc", false, "Skip MD5 checksum calculation on source and destination (use existing checksums)")

	// Register optional pprof flags (enabled only with -tags debugpprof)
	pprofAddFlags()

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS] <destination> [sources...]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Arguments:\n")
		fmt.Fprintf(os.Stderr, "  <destination>  Backup destination directory (required, default: current directory if omitted)\n")
		fmt.Fprintf(os.Stderr, "  [sources...]   Source directories to backup (optional, default: paths from config file)\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
	}

	flag.Parse()
	///////////////////////////////////
	// Logging setup
	f, exitCode := cli.SetupLogFile(LOGFILENAME)
	if exitCode != cli.ExitOk {
		fmt.Printf("error opening log file: %v\n", exitCode)
		retcode = exitCode
		return
	}
	defer f.(*os.File).Close()

	log.SetOutput(f)
	log.Println("This is a test log entry")

	// Log initial memory stats
	logMemoryStats("[STARTUP]")

	// Start periodic memory monitoring (every 30 seconds)
	monitorDone := make(chan struct{})
	defer close(monitorDone)
	startMemoryMonitor(30*time.Second, monitorDone)

	// Initialize optional pprof (no-op unless built with -tags debugpprof)
	onInterrupt := pprofInit(monitorDone, f)

	///////////////////////////////////
	// Read in top level config
	loader := cli.NewConfigLoader(*configPath, os.Stderr)
	xc, exitCode = loader.Load()
	if exitCode != cli.ExitOk {
		retcode = exitCode
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
	var dest string
	var sources []string
	if flag.NArg() > 0 {
		dest = flag.Arg(0)
	} else {
		dest = "."
	}

	if flag.NArg() > 1 {
		sources = flag.Args()[1:]
	} else {
		sources = xc.GetSourcePaths()
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
				logMemoryStats("[INTERRUPT]")
				if onInterrupt != nil {
					onInterrupt()
				}
				close(shutdownChan)
			} else {
				logMemoryStats("[FORCE EXIT]")
				os.Exit(1)
			}
		}
	}()

	///////////////////////////////////
	// Create config and run

	cfg := Config{
		ProjectConfig:  xc,
		Destination:    dest,
		Sources:        sources,
		ScanMode:       *scanflg,
		DummyMode:      *dummyflg,
		DeleteMode:     *delflg,
		StatsMode:      *statsflg,
		SkipCheckCalc:  *skipCheckCalcFlg,
		LogOutput:      f,
		MessageWriter:  os.Stdout,
		ShutdownChan:   shutdownChan,
		UseProgressBar: true,
	}

	logMemoryStats("[BEFORE RUN]")
	cli.ExitFromRun(Run(cfg))
	logMemoryStats("[AFTER RUN]")
}
