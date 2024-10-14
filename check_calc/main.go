package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/cbehopkins/medorg"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
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

type barDirHandler struct {
	dirBar               *mpb.Bar
	theMap               map[string]*mpb.Bar
	progressBars         *mpb.Progress
	suppressProgressBars bool
}

func NewBarDirHandler(suppressProgressBars bool) *barDirHandler {
	progressBars := mpb.New()
	bh := barDirHandler{}
	bh.suppressProgressBars = suppressProgressBars
	bh.progressBars = progressBars
	bh.dirBar = bh.progressBars.AddBar(
		0,
		mpb.PrependDecorators(decor.Name("Dirs")),
		mpb.AppendDecorators(decor.CountersNoUnit("%d / %d")),
	)
	bh.theMap = make(map[string]*mpb.Bar)
	return &bh
}
func (bh *barDirHandler) OpenDir(path string) {
	if bh.suppressProgressBars {
		return
	}
	bh.theMap[path] = bh.progressBars.AddBar(
		0,
		mpb.PrependDecorators(decor.Name(path)),
		mpb.AppendDecorators(decor.CountersNoUnit("%d / %d")),
	)
}
func (bh *barDirHandler) CloseDir(path string) {
	if bh.suppressProgressBars {
		return
	}
	bh.theMap[path].Abort(true)
	bh.theMap[path].Wait()
	delete(bh.theMap, path)
}
func getFileSize(filePath string) (int64, error) {
    fileInfo, err := os.Stat(filePath)
    if err != nil {
        return 0, err
    }
    return fileInfo.Size(), nil
}
// Define a struct that embeds the ReadCloser
type customReadCloser struct {
    io.ReadCloser
	md5CalcBar *mpb.Bar
	name string
}

// Implement the Read method to delegate to the embedded ReadCloser
func (crc *customReadCloser) Read(p []byte) (n int, err error) {
    i, err :=  crc.ReadCloser.Read(p)
	crc.md5CalcBar.IncrBy(i)
	return i, err
}

// Implement the Close method to perform additional actions
func (crc *customReadCloser) Close() error {
    defer func() {
		crc.md5CalcBar.Abort(true)
		crc.md5CalcBar.Wait()
    }()
    return crc.ReadCloser.Close()
}

func (bh *barDirHandler) FileVisitor(dir_tracker *medorg.DirTracker, directory, file string, dm medorg.DirectoryMap) *mpb.Bar {
	if bh.suppressProgressBars {
		return nil
	}
	bh.dirBar.SetCurrent(dir_tracker.Value())
	bh.dirBar.SetTotal(dir_tracker.Total(), false)
	_, ok := bh.theMap[directory]
	if !ok {
		panic("Path not found: " + directory)
	}
	bh.theMap[directory].SetTotal(int64(dm.Len()), false)
	bh.theMap[directory].Increment()

	fileSize, _ := getFileSize(directory + "/" + file)
	md5CalcBar := bh.progressBars.AddBar(fileSize,
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.Name("Calculating MD5: " + file),
			decor.CountersKibiByte("% .2f / % .2f"),
		),
		mpb.AppendDecorators(
			decor.Percentage(),
		),)
	return  md5CalcBar
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

	var logFilePath = flag.String("logfile", "", "Path to log file")
	flag.Parse()
	suppressProgressBars := false
	// Set up signal handling for Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	shutdown := false
	go func() {
		<-sigChan
		fmt.Println("\nCtrl-C received, shutting down...")
		shutdown = true
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
		var xc *medorg.XMLCfg
		if xmcf := medorg.XmConfig(); xmcf != "" {
			// FIXME should we be casting to string here or fixing the interfaces?
			xc = medorg.NewXMLCfg(string(xmcf))
		} else {
			fmt.Println("no config file found")
			fn := filepath.Join(string(medorg.HomeDir()), medorg.Md5FileName)
			xc = medorg.NewXMLCfg(fn)
		}
		AF = medorg.NewAutoFix(xc.Af)
		AF.DeleteFiles = *delflg
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
	tokenBuffer := makeCalcTokens(calcCnt)
	defer close(tokenBuffer)
	var dir_tracker *medorg.DirTracker
	pbs := NewBarDirHandler(suppressProgressBars)
	visitor := func(dm medorg.DirectoryMap, directory, file string, d fs.DirEntry) error {
		if shutdown {
			return medorg.ErrShutdown
		}
		if file == medorg.Md5FileName {
			return nil
		}
		fc := func(fs *medorg.FileStruct) error {
			mpbBar := pbs.FileVisitor(dir_tracker, directory, file, dm)
			defer func() {
				mpbBar.Abort(true)
				mpbBar.Wait()
			}()
			info, err := d.Info()
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
				<-tokenBuffer
				defer func() { tokenBuffer <- struct{}{} }()
				err = fs.ValidateChecksum()
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
			// Grab a compute token
			<-tokenBuffer
			defer func() { 
				tokenBuffer <- struct{}{}
			 }()

			err = fs.UpdateChecksum(*rclflg, func(r io.ReadCloser) io.ReadCloser {
				return &customReadCloser{r, mpbBar, file}
			})
			if errors.Is(err, medorg.ErrIOError) {
				fmt.Println("Received an IO error calculating checksum ", fs.Name, err)
				return nil
			}
			return err
		}
		err := dm.RunFsFc(directory, file, fc)
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
		if shutdown {return nil, medorg.ErrShutdown}
		mkFk := func(dir string) (medorg.DirectoryEntryInterface, error) {
			if shutdown {return nil, medorg.ErrShutdown}
			dm, err := medorg.DirectoryMapFromDir(dir)
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
		if shutdown {continue}
		if *conflg {
			con = &medorg.Concentrator{BaseDir: dir}
		}
		dir_tracker = medorg.NewDirTracker(dir, makerFunc)
		dir_tracker.PreserveStructs = false
		dir_tracker.OpenDirectoryCallback = func(path string, de medorg.DirectoryTrackerInterface) {
			pbs.OpenDir(path)
		}
		dir_tracker.CloseDirectoryCallback = func(path string, de medorg.DirectoryTrackerInterface) {
			pbs.CloseDir(path)
		}
		errChan := dir_tracker.Start().ErrChan()

		for err := range errChan {
			if errors.Is(err, medorg.ErrShutdown) { continue}
			fmt.Println("Error received while walking:", dir, err)
			os.Exit(2)
		}
	}
	fmt.Println("Finished walking")
}

func makeCalcTokens(calcCnt *int) chan struct{} {
	tokenBuffer := make(chan struct{}, *calcCnt)
	for i := 0; i < *calcCnt; i++ {
		tokenBuffer <- struct{}{}
	}
	return tokenBuffer
}
