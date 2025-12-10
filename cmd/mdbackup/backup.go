package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"sync"

	"github.com/cbehopkins/medorg/pkg/cli"
	"github.com/cbehopkins/medorg/pkg/consumers"
	"github.com/cbehopkins/medorg/pkg/core"
	pb "github.com/cbehopkins/pb/v3"
	bytesize "github.com/inhies/go-bytesize"
)

// SimpleVolumeLabelProvider implements consumers.VolumeLabeler using MdConfig
type SimpleVolumeLabelProvider struct {
	cfg *core.MdConfig
}

func (s SimpleVolumeLabelProvider) GetVolumeLabel(destDir string) (string, error) {
	vc, err := s.cfg.VolumeCfgFromDir(destDir)
	if err != nil {
		return "", err
	}
	return vc.Label, nil
}

// Config holds the configuration for mdbackup
type Config struct {
	// ProjectConfig holds project-level settings (ignore patterns etc.)
	ProjectConfig *core.MdConfig

	// Destination backup directory (target)
	Destination string
	// One or more source directories to back up into Destination
	Sources []string

	// TagMode        bool
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
	if cfg.ProjectConfig == nil {
		return cli.ExitNoConfig, errors.New("no project config")
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
			return cli.ExitProgressBar, fmt.Errorf("failed to start progress bar: %w", err)
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
		return cli.ExitOk, nil
	}

	// Main backup mode - requires destination and at least one source
	if cfg.Destination == "" || len(cfg.Sources) == 0 {
		return cli.ExitTwoDirectoriesOnly, fmt.Errorf("expected destination + at least 1 source, got dest='%s' sources=%d", cfg.Destination, len(cfg.Sources))
	}
	fileSkipper := func(path core.Fpath) bool {
		// Reuse ignore patterns defined in the project config if available
		if cfg.ProjectConfig != nil && cfg.ProjectConfig.ShouldIgnore(string(path)) {
			log.Println("Skipping (ignored):", path)
			return true
		}
		return false
	}

	// Setup the copier function
	var copyer func(src, dst core.Fpath) error
	if cfg.DummyMode {
		log.Println("Configuring for dummy copy mode")
		copyer = func(src, dst core.Fpath) error {
			if fileSkipper(src) {
				return nil
			}
			log.Println("Copy from:", src, " to ", dst)
			return consumers.ErrDummyCopy
		}
	} else if cfg.UseProgressBar {
		log.Println("Using Progress bar style copy")
		copyer = func(src, dst core.Fpath) error {
			if fileSkipper(src) {
				return nil
			}
			return poolCopier(src, dst, pool, &wg)
		}
	} else {
		log.Println("Configuring for default copy mode")
		copyer = func(src, dst core.Fpath) error {
			if fileSkipper(src) {
				return nil
			}
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
	fmt.Println("Finished configuring callbacks", len(cfg.Sources))

	// Choose backup strategy based on source count and delete mode
	// Use multi-source runner when we have multiple sources, as it handles orphan detection correctly
	if len(cfg.Sources) > 1 {
		setMessage("Starting Multi-Source Backup Run")
		volumeLabel := SimpleVolumeLabelProvider{cfg.ProjectConfig}
		err := consumers.BackupRunnerMultiSource(
			volumeLabel,
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
			return cli.ExitIncompleteBackup, err
		}
	} else {
		// Single source - use original BackupRunner
		volumeLabel := SimpleVolumeLabelProvider{cfg.ProjectConfig}
		for _, src := range cfg.Sources {
			setMessage("Starting Backup Run on " + src)
			err := consumers.BackupRunner(
				volumeLabel,
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
				return cli.ExitIncompleteBackup, err
			}
		}
	}

	setMessage("Waiting for complete")
	wg.Wait()

	return cli.ExitOk, nil
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
