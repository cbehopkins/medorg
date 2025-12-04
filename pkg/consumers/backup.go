package consumers

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/cbehopkins/medorg/pkg/core"
)

// ErrMissingEntry You are copying a file that there is no directory entry for. Probably need to rerun a visit on the directory
var ErrMissingEntry = errors.New("attempting to copy a file there seems to be no directory entry for")

// ErrDummyCopy Return this from your copy function to skip the effects of copying on the md5 files
var ErrDummyCopy = errors.New("not really copying, it's all good though")

// Export of the generic IO error from syscall
var (
	ErrIOError = syscall.Errno(5) // I don't like this, but don't know a better way
	// Export of no space left on device from syscall
	ErrNoSpace = syscall.Errno(28)
)

// VolumeLabeler provides volume label functionality for backup operations
type VolumeLabeler interface {
	GetVolumeLabel(destDir string) (string, error)
}

type backupKey struct {
	size     int64
	checksum string
}

// newBackupKeyFromFileStruct creates a backupKey from a FileStruct
func newBackupKeyFromFileStruct(fs core.FileStruct) backupKey {
	return backupKey{fs.Size, fs.Checksum}
}

// newBackupKeyFromMetadata creates a backupKey from a FileMetadata
func newBackupKeyFromMetadata(fm core.FileMetadata) backupKey {
	return backupKey{fm.GetSize(), fm.GetChecksum()}
}

type backupDupeMap struct {
	sync.Mutex
	dupeMap map[backupKey]core.Fpath
}

// Add an entry to the map (legacy concrete type version)
func (bdm *backupDupeMap) Add(fs core.FileStruct) {
	key := newBackupKeyFromFileStruct(fs)
	bdm.Lock()
	if bdm.dupeMap == nil {
		bdm.dupeMap = make(map[backupKey]core.Fpath)
	}
	bdm.dupeMap[key] = core.Fpath(fs.Path())
	bdm.Unlock()
}

// AddMetadata adds an entry using the FileMetadata interface
func (bdm *backupDupeMap) AddMetadata(fm core.FileMetadata) {
	key := newBackupKeyFromMetadata(fm)
	bdm.Lock()
	if bdm.dupeMap == nil {
		bdm.dupeMap = make(map[backupKey]core.Fpath)
	}
	bdm.dupeMap[key] = core.Fpath(fm.Path())
	bdm.Unlock()
}

func (bdm *backupDupeMap) Len() int {
	if bdm.dupeMap == nil {
		return 0
	}
	bdm.Lock()
	defer bdm.Unlock()
	return len(bdm.dupeMap)
}

// Remove an entry from the dumap
func (bdm *backupDupeMap) Remove(key backupKey) {
	if bdm.dupeMap == nil {
		return
	}
	bdm.Lock()
	delete(bdm.dupeMap, key)
	bdm.Unlock()
}

// Get an item from the map
func (bdm *backupDupeMap) Get(key backupKey) (core.Fpath, bool) {
	if bdm.dupeMap == nil {
		return "", false
	}
	bdm.Lock()
	defer bdm.Unlock()
	v, ok := bdm.dupeMap[key]
	return v, ok
}

func (bdm *backupDupeMap) AddVisit(dm core.DirectoryEntryInterface, dir, fn string, fileStruct core.FileStruct) error {
	bdm.Add(fileStruct)
	return nil
}

func (bdm *backupDupeMap) NewSrcVisitor(
	lookupFunc func(core.Fpath, bool) error,
	backupDestination *backupDupeMap, volumeName string,
) func(dm core.DirectoryEntryInterface, dir, fn string, fileStruct core.FileStruct) error {
	return func(dm core.DirectoryEntryInterface, dir, fn string, fileStruct core.FileStruct) error {
		// If it exists in the destination already
		path, ok := backupDestination.Get(newBackupKeyFromFileStruct(fileStruct))
		if lookupFunc != nil {
			err := lookupFunc(path, ok)
			if err != nil {
				return err
			}
		}
		if ok {
			// Then mark in the source as already backed up
			_ = fileStruct.AddTag(volumeName)
		}
		if !ok && fileStruct.HasTag(volumeName) {
			fileStruct.RemoveTag(volumeName)
		}

		bdm.Add(fileStruct)
		adm, _ := dm.(core.DirectoryMap)
		adm.Add(fileStruct)
		return nil
	}
}

type backScanner struct {
	dupeFunc   func(path string) error
	lookupFunc func(core.Fpath, bool) error
}

type (
	fpathList     []core.Fpath
	fpathListList []fpathList
)

func (fpll *fpathListList) Add(index int, fp core.Fpath) {
	for len(*fpll) <= index {
		*fpll = append(*fpll, fpathList{})
	}
	(*fpll)[index] = append((*fpll)[index], fp)
}

// streamingFileProcessor processes files in batches to avoid memory buildup
// It maintains priority ordering by processing files with fewer existing backups first
type streamingFileProcessor struct {
	srcDir          string
	destDir         string
	backupLabelName string
	fc              FileCopier
	maxNumBackups   int
	logFunc         func(msg string)
	shutdownChan    chan struct{}

	// Batching state
	batches    fpathListList
	batchSize  int
	copyTokens chan struct{}
	mu         sync.Mutex
	wg         sync.WaitGroup
	errChan    chan error
}

func newStreamingFileProcessor(
	srcDir, destDir, backupLabelName string,
	fc FileCopier,
	maxNumBackups int,
	logFunc func(msg string),
	shutdownChan chan struct{},
) *streamingFileProcessor {
	return &streamingFileProcessor{
		srcDir:          srcDir,
		destDir:         destDir,
		backupLabelName: backupLabelName,
		fc:              fc,
		maxNumBackups:   maxNumBackups,
		logFunc:         logFunc,
		shutdownChan:    shutdownChan,
		batches:         fpathListList{},
		batchSize:       1000, // Process files in chunks of 1000
		copyTokens:      core.MakeTokenChan(2),
		errChan:         make(chan error),
	}
}

// addFile adds a file to the current batch, potentially triggering a flush
func (sfp *streamingFileProcessor) addFile(numBackups int, fp core.Fpath) error {
	sfp.mu.Lock()
	sfp.batches.Add(numBackups, fp)

	// Count total files in all batches
	totalFiles := 0
	for _, batch := range sfp.batches {
		totalFiles += len(batch)
	}
	sfp.mu.Unlock()

	// If we've accumulated enough files, process a batch
	if totalFiles >= sfp.batchSize {
		return sfp.processBatch()
	}
	return nil
}

// processBatch processes the highest priority batch (fewest existing backups)
func (sfp *streamingFileProcessor) processBatch() error {
	sfp.mu.Lock()

	// Find first non-empty batch (these are ordered by priority)
	var filesToProcess fpathList
	for i, batch := range sfp.batches {
		if len(batch) > 0 {
			if i >= sfp.maxNumBackups {
				// Skip files that already have too many backups
				sfp.batches[i] = fpathList{}
				continue
			}
			filesToProcess = batch
			sfp.batches[i] = fpathList{}
			break
		}
	}
	sfp.mu.Unlock()

	if len(filesToProcess) == 0 {
		return nil
	}

	// Process this batch of files
	for _, file := range filesToProcess {
		select {
		case <-sfp.shutdownChan:
			return nil
		default:
		}

		// Get a token to limit concurrency
		<-sfp.copyTokens

		sfp.wg.Add(1)
		go func(file core.Fpath) {
			defer sfp.wg.Done()
			defer func() { sfp.copyTokens <- struct{}{} }()
			err := doACopy(sfp.srcDir, sfp.destDir, sfp.backupLabelName, file, sfp.fc)
			if err != nil {
				select {
				case sfp.errChan <- err:
				default:
				}
			}
		}(file)
	}

	return nil
}

// flush processes all remaining files
func (sfp *streamingFileProcessor) flush() error {
	// Process all remaining batches
	for {
		sfp.mu.Lock()
		hasFiles := false
		for _, batch := range sfp.batches {
			if len(batch) > 0 {
				hasFiles = true
				break
			}
		}
		sfp.mu.Unlock()

		if !hasFiles {
			break
		}

		if err := sfp.processBatch(); err != nil {
			return err
		}
	}

	// Wait for all copy operations to complete
	sfp.wg.Wait()
	close(sfp.copyTokens)
	close(sfp.errChan)

	// Check for any errors
	for err := range sfp.errChan {
		if errors.Is(err, ErrNoSpace) {
			sfp.logFunc("Destination full")
			return nil
		}
		if err != nil {
			return fmt.Errorf("copy failed, %w::%s, %s, %s", err, sfp.srcDir, sfp.destDir, sfp.backupLabelName)
		}
	}

	return nil
}

// scanBackupDirectories will mark srcDir's ArchiveAt
// tag, with any files that are already found in the destination
func (bs backScanner) scanBackupDirectories(
	destDir, srcDir, volumeName string,
	registerFunc func(*core.DirTracker),
	logFunc func(msg string),
	shutdownChan chan struct{},
) ([]*core.DirTracker, error) {
	if logFunc == nil {
		logFunc = func(msg string) {
			log.Println(msg)
		}
	}
	dta := core.AutoVisitFilesInDirectories([]string{destDir, srcDir}, nil)
	// Handle errors from directory traversal
	errChan := make(chan error, len(dta)) // Buffer with capacity = number of senders
	var wg sync.WaitGroup
	wg.Add(len(dta))
	for _, ndt := range dta {
		if registerFunc != nil {
			registerFunc(ndt)
		}
		go func(ndt *core.DirTracker) {
			for err := range ndt.ErrChan() {
				if err != nil {
					errChan <- err
				}
			}
			wg.Done()
		}(ndt)
	}
	go func() {
		wg.Wait()
		close(errChan)
	}()
	for err := range errChan {
		return nil, err
	}

	// No checksum calculation needed - RunCheckCalc already did it
	// Just read the existing directory maps which now have checksums

	var backupDestination backupDupeMap
	var backupSource backupDupeMap

	logFunc("Initial scan for anything that needs building")
	dta[0].RevisitAll(destDir, registerFunc, backupDestination.AddVisit, shutdownChan)
	logFunc("Scanning Source for Files already at destination")
	dta[1].RevisitAll(srcDir, registerFunc, backupSource.NewSrcVisitor(bs.lookupFunc, &backupDestination, volumeName), shutdownChan)
	logFunc("Dealing with duplicates")
	if (bs.dupeFunc != nil) && (backupDestination.Len() > 0) {
		// There's stuff on the backup that's not in the Source
		// We'll need to do something about this soon!
		// log.Println("Unexpected items left in backup destination")
		for _, v := range backupDestination.dupeMap {
			if err := bs.dupeFunc(string(v)); err != nil {
				return nil, err
			}
		}
	}
	return dta, nil
}

// extractCopyFiles will look for files that are not backed up
// i.e. walk through src file system looking for files
// That don't have the volume name as an archived at
//
// NOTE: This function is kept for testing purposes to validate file extraction logic.
// Production code uses streamingExtractAndCopy to avoid memory buildup.
func extractCopyFiles(srcDir string, dt *core.DirTracker, volumeName string, registerFunc func(*core.DirTracker), maxNumBackups int, shutdownChan chan struct{}) (fpathListList, error) {
	var lk sync.Mutex
	remainingFiles := fpathListList{}
	visitFunc := func(dm core.DirectoryEntryInterface, dir, fn string, fileStruct core.FileStruct) error {
		// Skip metadata files - these should never be backed up
		if fn == core.Md5FileName || fn == ".mdjournal.xml" || fn == ".mdbackup.xml" {
			return nil
		}

		if fileStruct.HasTag(volumeName) {
			return nil
		}
		fp := core.NewFpath(dir, fn)
		lenArchive := len(fileStruct.BackupDest)
		if lenArchive > maxNumBackups {
			return nil
		}
		lk.Lock()
		remainingFiles.Add(lenArchive, fp)
		lk.Unlock()
		return nil
	}
	dt.RevisitAll(srcDir, registerFunc, visitFunc, nil)
	return remainingFiles, nil
}

// streamingExtractAndCopy walks the source directory and processes files in batches
// to avoid memory buildup when backing up large collections
func streamingExtractAndCopy(
	srcDir, destDir, volumeName string,
	dt *core.DirTracker,
	registerFunc func(*core.DirTracker),
	fc FileCopier,
	maxNumBackups int,
	logFunc func(msg string),
	shutdownChan chan struct{},
) error {
	processor := newStreamingFileProcessor(
		srcDir, destDir, volumeName,
		fc, maxNumBackups,
		logFunc, shutdownChan,
	)

	visitFunc := func(dm core.DirectoryEntryInterface, dir, fn string, fileStruct core.FileStruct) error {
		// Skip metadata files - these should never be backed up
		if fn == core.Md5FileName || fn == ".mdjournal.xml" || fn == ".mdbackup.xml" {
			return nil
		}

		if fileStruct.HasTag(volumeName) {
			return nil
		}
		fp := core.NewFpath(dir, fn)
		lenArchive := len(fileStruct.BackupDest)
		if lenArchive > maxNumBackups {
			return nil
		}

		// Add to processor, which handles batching automatically
		return processor.addFile(lenArchive, fp)
	}

	dt.RevisitAll(srcDir, registerFunc, visitFunc, nil)

	// Flush any remaining files
	return processor.flush()
}

type FileCopier func(src, dst core.Fpath) error

func doACopy(
	srcDir, // The source of the backup as specified on the command line
	destDir, // The destination directory as specified...
	backupLabelName string, // the tag we should add to the sorce
	file core.Fpath, // The full path of the file
	fc FileCopier,
) error {
	if fc == nil {
		fc = core.CopyFile
	}

	// Workout the new path the target file should have
	// this is relative to the srcdir so that
	// the dst dir keeps the hierarchy
	rel, err := filepath.Rel(srcDir, string(file))
	if err != nil {
		return err
	}

	// Actually copy the file
	err = fc(file, core.NewFpath(destDir, rel))
	if errors.Is(err, ErrDummyCopy) {
		return nil
	}
	if errors.Is(err, ErrNoSpace) {
		_ = core.RmFilename(core.NewFpath(destDir, rel))
		return ErrNoSpace
	}
	// Update the srcDir .md5 file with the fact we've backed this up now
	basename := filepath.Base(string(file))
	sd := filepath.Dir(string(file))
	if err != nil {
		return err
	}
	dmSrc, err := core.DirectoryMapFromDir(sd)
	if err != nil {
		return err
	}
	src, ok := dmSrc.Get(basename)
	if !ok {
		return fmt.Errorf("%w: %s, \"%s\" \"%s\"", ErrMissingEntry, file, sd, basename)
	}
	_ = src.AddTag(backupLabelName)
	dmSrc.Add(src)
	if err := dmSrc.Persist(sd); err != nil {
		return err
	}
	_ = src.RemoveTag(backupLabelName)
	// Update the destDir with the checksum from the srcDir
	dmDst, err := core.DirectoryMapFromDir(destDir)
	if err != nil {
		return err
	}
	fs, err := os.Stat(filepath.Join(destDir, rel))
	if err != nil {
		return err
	}
	src.SetDirectory(destDir)
	src.Mtime = fs.ModTime().Unix()
	dmDst.Add(src)
	return dmDst.Persist(destDir)
}

// doCopies processes a pre-collected list of files to copy
//
// NOTE: This function is kept for testing purposes.
// Production code uses streamingExtractAndCopy for memory-efficient streaming.
func doCopies(
	srcDir, destDir string,
	backupLabelName string,
	fc FileCopier,
	copyFilesArray fpathListList, maxNumBackups int,
	logFunc func(msg string), shutdownChan chan struct{},
) error {
	// I don't like this pattern as it's not a clean pipeline - but the alternatives feel worse
	copyTokens := core.MakeTokenChan(2)
	copyErrChan := make(chan error, 2) // Buffer with capacity = number of concurrent goroutines (matches token limit)
	var cwg sync.WaitGroup
	go func() {
		defer func() {
			cwg.Wait()
			close(copyErrChan)
		}()
		// Now do the copy, updating srcDir's labels as we go
		for numBackups, copyFiles := range copyFilesArray {
			if numBackups >= maxNumBackups {
				logFunc(fmt.Sprint("Not backing up to more than", maxNumBackups, "places"))
				return
			}
			for _, file := range copyFiles {
				select {
				case <-shutdownChan:
					logFunc("Seen shutdown request")
					return

				case _, ok := <-copyTokens:
					if !ok {
						return
					}
				}

				cwg.Add(1)
				go func(file core.Fpath) {
					copyErrChan <- doACopy(srcDir, destDir, backupLabelName, file, fc)
					cwg.Done()
				}(file)
			}
		}
	}()
	defer func() { close(copyTokens) }()
	for err := range copyErrChan {
		if errors.Is(err, ErrNoSpace) {
			// FIXME in the ideal world, we'd look at how much space there is left on the volume
			// and look for a file with a size smaller than that
			// and copy that.
			// For now, that optimization is not too bad.
			logFunc("Destination full")
			return nil
		}
		if err != nil {
			return fmt.Errorf("copy failed, %w::%s, %s, %s", err, srcDir, destDir, backupLabelName)
		}
		copyTokens <- struct{}{}
	}
	return nil
}

// BackupRunnerMultiSource handles backup from multiple source directories to a single destination
// with proper orphan detection across all sources. This should be used when len(srcDirs) > 1 and
// orphan detection is enabled.
func BackupRunnerMultiSource(
	xc VolumeLabeler,
	maxNumBackups int,
	fc FileCopier,
	srcDirs []string,
	destDir string,
	orphanFunc func(path string) error,
	logFunc func(msg string),
	registerFunc func(*core.DirTracker),
	shutdownChan chan struct{},
) error {
	if logFunc == nil {
		logFunc = func(msg string) {
			log.Println(msg)
		}
	}
	backupLabelName, err := xc.GetVolumeLabel(destDir)
	if err != nil {
		return err
	}
	logFunc(fmt.Sprint("Determined label as: \"", backupLabelName, "\" :now scanning directories"))

	// Step 1: Run mdcalc to calculate/update all MD5 checksums
	// This ensures destination and all sources have up-to-date .medorg.xml files
	allDirs := make([]string, 0, 1+len(srcDirs))
	allDirs = append(allDirs, destDir)
	allDirs = append(allDirs, srcDirs...)

	checkCalcOpts := CheckCalcOptions{
		CalcCount: len(allDirs), // Parallel processing for all directories
		Recalc:    false,
		Validate:  false,
		Scrub:     false,
		AutoFix:   nil,
	}
	logFunc("Running mdcalc on all directories (destination + sources)")
	if err := RunCheckCalc(allDirs, checkCalcOpts); err != nil {
		return fmt.Errorf("error running mdcalc: %w", err)
	}

	// Step 2: Scan all directories (checksums already calculated)
	dta := core.AutoVisitFilesInDirectories(allDirs, nil)

	// Handle errors from directory traversal
	errChan := make(chan error, len(dta)) // Buffer with capacity = number of senders
	var wg sync.WaitGroup
	wg.Add(len(dta))
	for _, ndt := range dta {
		if registerFunc != nil {
			registerFunc(ndt)
		}
		go func(ndt *core.DirTracker) {
			for err := range ndt.ErrChan() {
				if err != nil {
					errChan <- err
				}
			}
			wg.Done()
		}(ndt)
	}
	go func() {
		wg.Wait()
		close(errChan)
	}()
	for err := range errChan {
		return err
	}

	// No checksum calculation needed - RunCheckCalc already did it
	// Build destination file map
	var backupDestination backupDupeMap
	logFunc("Initial scan for anything that needs building")
	dta[0].RevisitAll(destDir, registerFunc, backupDestination.AddVisit, shutdownChan)

	// Scan all sources and mark files found in destination
	for i, srcDir := range srcDirs {
		logFunc(fmt.Sprintf("Scanning Source %d for Files already at destination", i+1))
		// Create a visitor that removes matching files from backupDestination
		removeMatchingVisitor := func(dm core.DirectoryEntryInterface, dir, fn string, fileStruct core.FileStruct) error {
			key := newBackupKeyFromFileStruct(fileStruct)
			_, ok := backupDestination.Get(key)
			if ok {
				// File exists in destination - mark as backed up and remove from orphan candidates
				_ = fileStruct.AddTag(backupLabelName)
				backupDestination.Remove(key)
			}
			return nil
		}
		dta[i+1].RevisitAll(srcDir, registerFunc, removeMatchingVisitor, shutdownChan)
	} // Now handle orphans - files in destination but not in ANY source
	logFunc("Dealing with duplicates")
	if (orphanFunc != nil) && (backupDestination.Len() > 0) {
		for _, v := range backupDestination.dupeMap {
			if err := orphanFunc(string(v)); err != nil {
				return err
			}
		}
	}

	// If scan-only mode, we're done
	if fc == nil {
		logFunc("Scan only. Going no further")
		return nil
	}

	// Now copy files from each source using streaming approach
	for i, srcDir := range srcDirs {
		logFunc(fmt.Sprintf("Looking for files to copy from source %d", i+1))

		err = streamingExtractAndCopy(
			srcDir, destDir, backupLabelName,
			dta[i+1],
			registerFunc,
			fc,
			maxNumBackups,
			logFunc,
			shutdownChan,
		)
		if err != nil {
			return fmt.Errorf("BackupRunnerMultiSource failed copying from source %d, %w", i+1, err)
		}

		logFunc(fmt.Sprintf("Finished Copy from source %d", i+1))
	}

	return nil
}

func BackupRunner(
	xc VolumeLabeler,
	maxNumBackups int,
	fc FileCopier,
	srcDir, destDir string,
	orphanFunc func(path string) error,
	logFunc func(msg string),
	registerFunc func(*core.DirTracker),
	shutdownChan chan struct{},
) error {
	if logFunc == nil {
		logFunc = func(msg string) {
			log.Println(msg)
		}
	}
	backupLabelName, err := xc.GetVolumeLabel(destDir)
	if err != nil {
		return err
	}
	logFunc(fmt.Sprint("Determined label as: \"", backupLabelName, "\" :now scanning directories"))

	// Step 1: Run mdcalc to calculate/update all MD5 checksums
	// This ensures both source and destination have up-to-date .medorg.xml files
	checkCalcOpts := CheckCalcOptions{
		CalcCount: 2, // Default parallelism
		Recalc:    false,
		Validate:  false,
		Scrub:     false,
		AutoFix:   nil,
	}
	logFunc("Running mdcalc on source and destination")
	if err := RunCheckCalc([]string{srcDir, destDir}, checkCalcOpts); err != nil {
		return fmt.Errorf("error running mdcalc: %w", err)
	}

	// Step 2: Scan directories and mark files (no checksum calculation needed)
	// Go ahead and run a mdcalc style scan of the directories and make sure
	// they have all their existing md5s up to date
	// First of all get the srcDir updated with files that are already in destDir
	var bs backScanner
	bs.dupeFunc = orphanFunc
	dt, err := bs.scanBackupDirectories(destDir, srcDir, backupLabelName, registerFunc, logFunc, shutdownChan)
	if err != nil {
		return err
	}
	if fc == nil {
		logFunc("Scan only. Going no further")
		// If we've not supplied a copier, when we clearly don't want to run the copy
		return nil
	}
	logFunc("Looking for files to  copy")

	err = streamingExtractAndCopy(
		srcDir, destDir, backupLabelName,
		dt[1],
		registerFunc,
		fc,
		maxNumBackups,
		logFunc,
		shutdownChan,
	)
	if err != nil {
		return fmt.Errorf("BackupRunner failed, %w", err)
	}

	logFunc("Finished Copy")
	return nil
}
