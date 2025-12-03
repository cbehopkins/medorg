package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"sync"

	"github.com/cbehopkins/medorg/pkg/consumers"
	"github.com/cbehopkins/medorg/pkg/core"
	pb "github.com/cbehopkins/pb/v3"
	bytesize "github.com/inhies/go-bytesize"
)

// VolumeConfigProvider provides volume configuration for directories
type VolumeConfigProvider interface {
	VolumeCfgFromDir(dir string) (*core.VolumeCfg, error)
	GetVolumeLabel(destDir string) (string, error)
}

// Config holds the configuration for mdbackup
type Config struct {
	// Destination backup directory (target)
	Destination string
	// One or more source directories to back up into Destination
	Sources []string

	// VolumeConfigProvider provides volume configuration for directories
	VolumeConfigProvider VolumeConfigProvider

	TagMode        bool
	ScanMode       bool
	DummyMode      bool
	DeleteMode     bool
	StatsMode      bool
	LogOutput      io.Writer
	MessageWriter  io.Writer
	ShutdownChan   chan struct{}
	UseProgressBar bool
}

// Run is the main backup logic, extracted for testability
// Returns exit code and error
func Run(cfg Config) (int, error) {
	// Set default outputs if not provided
	if cfg.LogOutput == nil {
		cfg.LogOutput = os.Stderr
	}
	if cfg.MessageWriter == nil {
		cfg.MessageWriter = os.Stdout
	}
	if cfg.ShutdownChan == nil {
		cfg.ShutdownChan = make(chan struct{})
	}

	// Configure logging
	log.SetOutput(cfg.LogOutput)

	// Validate config
	if cfg.VolumeConfigProvider == nil {
		return ExitNoConfig, errors.New("no volume config provider")
	}

	// Setup progress bar or simple logging
	var pool *pb.Pool
	var messageBar *pb.ProgressBar
	var logBar *pb.ProgressBar
	var wg sync.WaitGroup

	if cfg.UseProgressBar {
		messageBar = new(pb.ProgressBar)
		pool = pb.NewPool(messageBar)
		err := pool.Start()
		if err != nil {
			return ExitProgressBar, fmt.Errorf("failed to start progress bar: %w", err)
		}
		defer pool.Stop()
		defer messageBar.Finish()
		messageBar.SetTemplateString(`{{string . "msg"}}`)
		messageBar.Set("msg", "Initializing")

		logBar = new(pb.ProgressBar)
		defer logBar.Finish()
		logBar.SetTemplateString(`{{string . "msg"}}`)
		pool.Add(logBar)
	}

	logFunc := func(msg string) {
		if cfg.UseProgressBar && logBar != nil {
			logBar.Set("msg", msg)
		}
		log.Println(msg)
	}

	setMessage := func(msg string) {
		if cfg.UseProgressBar && messageBar != nil {
			messageBar.Set("msg", msg)
		} else {
			fmt.Fprintln(cfg.MessageWriter, msg)
		}
	}

	// Handle tag mode (configure destination only)
	if cfg.TagMode {
		if cfg.Destination == "" {
			return ExitOneDirectoryOnly, errors.New("destination directory required when configuring tags")
		}
		vc, err := cfg.VolumeConfigProvider.VolumeCfgFromDir(cfg.Destination)
		if err != nil {
			return ExitBadVc, fmt.Errorf("failed to get volume config: %w", err)
		}
		fmt.Fprintf(cfg.MessageWriter, "Config name is %s\n", vc.Label)
		return ExitOk, nil
	}

	// Handle stats mode (scan destination + sources)
	if cfg.StatsMode {
		dirs := make([]string, 0, 1+len(cfg.Sources))
		if cfg.Destination != "" {
			dirs = append(dirs, cfg.Destination)
		}
		dirs = append(dirs, cfg.Sources...)
		if cfg.UseProgressBar {
			runStats(pool, messageBar, dirs)
		} else {
			runStatsSimple(dirs, setMessage, logFunc)
		}
		return ExitOk, nil
	}

	// Main backup mode - requires destination and at least one source
	if cfg.Destination == "" || len(cfg.Sources) == 0 {
		return ExitTwoDirectoriesOnly, fmt.Errorf("expected destination + at least 1 source, got dest='%s' sources=%d", cfg.Destination, len(cfg.Sources))
	}

	// Setup the copier function
	var copyer func(src, dst core.Fpath) error
	if cfg.DummyMode {
		copyer = func(src, dst core.Fpath) error {
			log.Println("Copy from:", src, " to ", dst)
			return consumers.ErrDummyCopy
		}
	} else if cfg.UseProgressBar {
		copyer = func(src, dst core.Fpath) error {
			return poolCopier(src, dst, pool, &wg)
		}
	} else {
		copyer = func(src, dst core.Fpath) error {
			log.Println("Copying:", src, "to", dst)
			return core.CopyFile(src, dst)
		}
	}

	if cfg.ScanMode {
		copyer = nil
	}

	// Setup orphaned file handler
	var orphanedFunc func(string) error
	if cfg.DummyMode {
		orphanedFunc = func(path string) error {
			log.Println(path, "orphaned")
			return nil
		}
	} else if cfg.DeleteMode {
		orphanedFunc = func(path string) error {
			log.Println(path, "orphaned")
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				_ = os.Remove(path)
			}
			return nil
		}
	}

	// Setup progress tracking
	var registerFunc func(*core.DirTracker)
	if cfg.UseProgressBar {
		registerFunc = func(dt *core.DirTracker) {
			topRegisterFunc(dt, pool, &wg)
		}
	}

	// Choose backup strategy based on source count and delete mode
	// Use multi-source runner when we have multiple sources, as it handles orphan detection correctly
	if len(cfg.Sources) > 1 {
		setMessage("Starting Multi-Source Backup Run")
		err := consumers.BackupRunnerMultiSource(
			cfg.VolumeConfigProvider,
			2,
			copyer,
			cfg.Sources,
			cfg.Destination,
			orphanedFunc,
			logFunc,
			registerFunc,
			cfg.ShutdownChan,
		)
		setMessage("Completed Multi-Source Backup Run")

		if err != nil {
			setMessage(fmt.Sprint("Unable to complete backup:", err))
			return ExitIncompleteBackup, err
		}
	} else {
		// Single source - use original BackupRunner
		for _, src := range cfg.Sources {
			setMessage("Starting Backup Run")
			err := consumers.BackupRunner(
				cfg.VolumeConfigProvider,
				2,
				copyer,
				src,
				cfg.Destination,
				orphanedFunc,
				logFunc,
				registerFunc,
				cfg.ShutdownChan,
			)
			setMessage("Completed Backup Run")

			if err != nil {
				setMessage(fmt.Sprint("Unable to complete backup:", err))
				return ExitIncompleteBackup, err
			}
		}
	}

	setMessage("Waiting for complete")
	wg.Wait()

	return ExitOk, nil
}

// runStatsSimple runs statistics without progress bar
func runStatsSimple(directories []string, setMessage func(string), logFunc func(string)) {
	setMessage("Start Scanning")
	var lk sync.Mutex
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
		totalArray[lenArchive] += int64(fileSize)
		lk.Unlock()
		return nil
	}

	errChan := core.VisitFilesInDirectories(directories, nil, visitFunc)
	for err := range errChan {
		logFunc(fmt.Sprint("Error Got...", err))
	}

	for i, val := range totalArray {
		b := bytesize.New(float64(val))
		logFunc(fmt.Sprintf("%d requires %s bytes", i, b))
	}
}
