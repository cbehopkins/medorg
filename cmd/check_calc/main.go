package main

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/cbehopkins/medorg/pkg/core"
)

func isDir(fn string) bool {
	stat, err := os.Stat(fn)
	if err != nil {
		return false
	}
	return stat.IsDir()
}

func main() {
	var directories []string

	scrubflg := flag.Bool("scrub", false, "Scruball backup labels from src records")

	calcCnt := flag.Int("calc", 2, "Max Number of MD5 calculators")
	delflg := flag.Bool("delete", false, "Delete duplicated Files")
	mvdflg := flag.Bool("mvd", false, "Move Detect")
	rnmflg := flag.Bool("rename", false, "Auto Rename Files")
	rclflg := flag.Bool("recalc", false, "Recalculate all checksums")
	valflg := flag.Bool("validate", false, "Validate all checksums")

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

	var AF *core.AutoFix
	if *rnmflg {
		var xc *core.XMLCfg
		var err error
		if xmcf := core.XmConfig(); xmcf != "" {
			// FIXME should we be casting to string here or fixing the interfaces?
			xc, err = core.NewXMLCfg(string(xmcf))
			if err != nil {
				fmt.Println("Error loading config file:", err)
				os.Exit(5)
			}
		} else {
			fmt.Println("no config file found")
			fn := filepath.Join(string(core.HomeDir()), core.Md5FileName)
			xc, err = core.NewXMLCfg(fn)
			if err != nil {
				fmt.Println("Error creating config file:", err)
				os.Exit(5)
			}
		}
		AF = core.NewAutoFix(xc.Af)
		AF.DeleteFiles = *delflg
	}

	if *mvdflg {
		err := core.RunMoveDetect(directories)
		if err != nil {
			fmt.Println("Error! In move detect", err)
			os.Exit(4)
		}
		fmt.Println("Finished move detection")
	}

	// Have a buffer of compute tokens
	// to ensure we're not doing too much at once
	tokenBuffer := make(chan struct{}, *calcCnt)
	defer close(tokenBuffer)
	for i := 0; i < *calcCnt; i++ {
		tokenBuffer <- struct{}{}
	}

	visitor := func(dm core.DirectoryMap, directory, file string, d fs.DirEntry) error {
		if file == core.Md5FileName {
			return nil
		}

		fc := func(fs *core.FileStruct) error {
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
				if errors.Is(err, core.ErrRecalced) {
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
			defer func() { tokenBuffer <- struct{}{} }()
			err = fs.UpdateChecksum(*rclflg)
			if errors.Is(err, core.ErrIOError) {
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
		return err
	}

	makerFunc := func(dir string) (core.DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (core.DirectoryEntryInterface, error) {
			dm, err := core.DirectoryMapFromDir(dir)
			if err != nil {
				return dm, err
			}
			dm.VisitFunc = visitor
			return dm, dm.DeleteMissingFiles()
		}
		de, err := core.NewDirectoryEntry(dir, mkFk)
		return de, err
	}
	for _, dir := range directories {
		errChan := core.NewDirTracker(false, dir, makerFunc).ErrChan()

		for err := range errChan {
			fmt.Println("Error received while walking:", dir, err)
			os.Exit(2)
		}
	}
	fmt.Println("Finished walking")
}
