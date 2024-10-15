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
	"runtime/pprof"
	"syscall"

	"github.com/cbehopkins/medorg"
)

func setupLoggingToFile(filename string) (*os.File, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return nil, err
	}
	log.SetOutput(file)
	return file, nil
}

func setupLoggingToNull() error {
	nullFile, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	log.SetOutput(nullFile)
	return nil
}
func setupLogging(logFilePath string) (*os.File, error) {
	if logFilePath == "" {
		return nil, setupLoggingToNull()
		// return nil,nil
	}
	return setupLoggingToFile(logFilePath)
}
func isDir(fn string) bool {
	stat, err := os.Stat(fn)
	if err != nil {
		return false
	}
	return stat.IsDir()
}

func main() {
	var directories []string

	var scrubflg = flag.Bool("scrub", false, "Scruball backup labels from src records")
	var calcCnt = flag.Int("calc", 2, "Max Number of MD5 calculators")
	var delflg = flag.Bool("delete", false, "Delete duplicated Files")
	var mvdflg = flag.Bool("mvd", false, "Move Detect")
	var rnmflg = flag.Bool("rename", false, "Auto Rename Files")
	var rclflg = flag.Bool("recalc", false, "Recalculate all checksums")
	var valflg = flag.Bool("validate", false, "Validate all checksums")
	var conflg = flag.Bool("conc", false, "Concentrate files together in same directory")
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	var logFilePath = flag.String("logfile", "", "Path to log file")
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	suppressProgressBars := false
	// Set up signal handling for Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	shutdownChan := make(chan struct{})

	go func() {
		<-sigChan
		log.Println("Ctrl-C received, shutting down...")
		close(shutdownChan)
	}()
	///////////////////////////////////
	// Setup logging
	logFile, err := setupLogging(*logFilePath)
	if err != nil {
		log.Fatalf("Failed to set up log file: %v", err)
	}
	if logFile != nil {
		defer logFile.Close()
	}

	///////////////////////////////////
	// Determine directories to walk
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
	// Run AutoFix
	var AF *medorg.AutoFix
	if *rnmflg {
		AF = configAutofix(AF, *delflg)
	}

	///////////////////////////////////
	// Run Move Detect
	if *mvdflg {
		err := medorg.RunMoveDetect(directories)
		if err != nil {
			fmt.Println("Error! In move detect", err)
			os.Exit(4)
		}
		fmt.Println("Finished move detection")
	}

	///////////////////////////////////
	// Run the main walk
	var con *medorg.Concentrator
	cpuTokens := makeCalcTokens(*calcCnt)
	ioTokens := makeCalcTokens(16)
	defer close(cpuTokens)
	var dir_tracker *medorg.DirTracker
	progBar := medorg.NewBarDirHandler(suppressProgressBars)
	visitor := func(dm medorg.DirectoryMap, directory, file string, d fs.DirEntry) error {
		if file == medorg.Md5FileName {
			return nil
		}
		fsBuilder := func(fs *medorg.FileStruct) error {
			readCloserWrap := progBar.FileVisitor(dir_tracker, directory, file, dm)
			<-ioTokens
			info, err := d.Info()
			ioTokens <- struct{}{}
			if err != nil {
				return err
			}
			changed, err := fs.Changed(info)
			if err != nil {
				return err
			}
			if *scrubflg {
				if len(fs.BackupDest) > 0 {
					changed = true
					fs.BackupDest = []string{}
				}
			}
			if *valflg {
				<-cpuTokens
				err = fs.ValidateChecksum(readCloserWrap)
				cpuTokens <- struct{}{}
				if errors.Is(err, medorg.ErrRecalced) {
					fmt.Println("Had to recalculate a checksum", fs.Name)
					return nil
				}
				return err
			}

			if !(changed || *rclflg || fs.Checksum == "") {
				// if we have no reason to recalculate
				return nil
			}

			fs.FromStat(directory, file, info)
			select {
			case <-shutdownChan:
				return medorg.ErrShutdown
			case <-cpuTokens:
				defer func() {
					cpuTokens <- struct{}{}
				}()
			}
			err = fs.UpdateChecksum(*rclflg, readCloserWrap)
			if errors.Is(err, medorg.ErrIOError) {
				fmt.Println("Received an IO error calculating checksum ", fs.Name, err)
				return nil
			}
			return err
		}
		err := dm.RunFsFc(directory, file, fsBuilder)
		if err != nil {
			return err
		}
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
			dm, err := medorg.DirectoryMapFromDir(dir, ioTokens)
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
		de, err := medorg.NewDirectoryEntry(dir, mkFk)
		return de, err
	}

	for _, dir := range directories {
		dir, err := filepath.Abs(dir)
		if err != nil {
			log.Fatalf("Failed to resolve path: %v", err)
		}
		select {
		case <-shutdownChan:
			continue
		default:
		}
		if *conflg {
			con = &medorg.Concentrator{BaseDir: dir}
		}
		dir_tracker = medorg.NewDirTracker(dir, makerFunc, ioTokens)
		dir_tracker.ShutdownChan = shutdownChan
		dir_tracker.PreserveStructs = false
		dir_tracker.OpenDirectoryCallback = func(path string, de medorg.DirectoryTrackerInterface) {
			progBar.OpenDir(path)
		}
		dir_tracker.CloseDirectoryCallback = func(path string, de medorg.DirectoryTrackerInterface) {
			progBar.CloseDir(path)
		}
		errChan := dir_tracker.Start().ErrChan()

		for err := range errChan {
			if errors.Is(err, medorg.ErrShutdown) {
				continue
			}
			fmt.Println("Error received while walking:", dir, err)
			os.Exit(2)
		}
	}
	fmt.Println("Finished walking")
}

func configAutofix(AF *medorg.AutoFix, delflg bool) *medorg.AutoFix {
	var xc *medorg.XMLCfg
	if xmcf := medorg.XmConfig(); xmcf != "" {

		xc = medorg.NewXMLCfg(string(xmcf))
	} else {
		fmt.Println("no config file found")
		fn := filepath.Join(string(medorg.HomeDir()), medorg.Md5FileName)
		xc = medorg.NewXMLCfg(fn)
	}
	AF = medorg.NewAutoFix(xc.Af)
	AF.DeleteFiles = delflg
	return AF
}

func makeCalcTokens(calcCnt int) chan struct{} {
	tokenBuffer := make(chan struct{}, calcCnt)
	for i := 0; i < calcCnt; i++ {
		tokenBuffer <- struct{}{}
	}
	return tokenBuffer
}
