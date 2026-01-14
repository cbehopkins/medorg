package consumers

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/cbehopkins/medorg/pkg/core"
	pb "github.com/cbehopkins/pb/v3"
)

// ErrMissingEntry You are copying a file that there is no directory entry for. Probably need to rerun a visit on the directory
var ErrMissingEntry = errors.New("attempting to copy a file there seems to be no directory entry for")

// ErrDummyCopy Return this from your copy function to skip the effects of copying on the md5 files
var ErrDummyCopy = errors.New("not really copying, it's all good though")

// Export of the generic IO error from syscall
var (
	ErrIOError = syscall.Errno(5) // I don't like this, but don't know a better way
	// Export of no space left on device from syscall
	// FIXME what's the windows equivalent?
	ErrNoSpace = syscall.Errno(28)
)

// VolumeLabeler provides volume label functionality for backup operations
type VolumeLabeler interface {
	GetVolumeLabel(destDir string) (string, error)
}

// directoryLocks provides per-directory mutex locks to prevent concurrent
// DirectoryMap updates from creating race conditions
type directoryLocks struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

// globalDirLocks is the global directory lock manager
var globalDirLocks = &directoryLocks{
	locks: make(map[string]*sync.Mutex),
}

// getLock returns a mutex for the given directory path
func (dl *directoryLocks) getLock(dir string) *sync.Mutex {
	dl.mu.Lock()
	defer dl.mu.Unlock()
	dir = filepath.Clean(dir)

	if dl.locks[dir] == nil {
		dl.locks[dir] = &sync.Mutex{}
	}
	return dl.locks[dir]
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

type checksumRecord struct {
	path            core.Fpath
	fromDestination bool
}

type checksumSet struct {
	mu   sync.Mutex
	seen map[string]checksumRecord
}

func newChecksumSet() *checksumSet {
	return &checksumSet{seen: make(map[string]checksumRecord)}
}

// SeedFromDestination marks a checksum as already present in the destination.
func (cs *checksumSet) SeedFromDestination(checksum string, path core.Fpath) {
	cs.mu.Lock()
	cs.seen[checksum] = checksumRecord{path: path, fromDestination: true}
	cs.mu.Unlock()
}

// Mark records a checksum that will be satisfied by the current run.
// Returns true if this is the first time we've seen the checksum.
func (cs *checksumSet) Mark(checksum string, path core.Fpath) bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if _, ok := cs.seen[checksum]; ok {
		return false
	}
	cs.seen[checksum] = checksumRecord{path: path}
	return true
}

func (cs *checksumSet) Get(checksum string) (checksumRecord, bool) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.seen == nil {
		return checksumRecord{}, false
	}
	rec, ok := cs.seen[checksum]
	return rec, ok
}

// Add an entry to the map (legacy concrete type version)
func (bdm *backupDupeMap) Add(fs core.FileStruct) {
	key := newBackupKeyFromFileStruct(fs)
	bdm.Lock()
	if bdm.dupeMap == nil {
		bdm.dupeMap = make(map[backupKey]core.Fpath)
	}
	bdm.dupeMap[key] = fs.Path()
	bdm.Unlock()
}

// AddMetadata adds an entry using the FileMetadata interface
func (bdm *backupDupeMap) AddMetadata(fm core.FileMetadata) {
	key := newBackupKeyFromMetadata(fm)
	bdm.Lock()
	if bdm.dupeMap == nil {
		bdm.dupeMap = make(map[backupKey]core.Fpath)
	}
	bdm.dupeMap[key] = fm.Path()
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
		return core.Fpath{}, false
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
			if added := fileStruct.AddTag(volumeName); !added {
				return fmt.Errorf("failed to add tag %s", volumeName)
			}
		}
		if !ok && fileStruct.HasTag(volumeName) {
			if removed := fileStruct.RemoveTag(volumeName); !removed {
				return fmt.Errorf("failed to remove tag %s", volumeName)
			}
		}

		bdm.Add(fileStruct)
		adm, _ := dm.(core.DirectoryMap)
		adm.Add(fileStruct)
		return nil
	}
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

// extractAndCopy performs backup using streaming visitor pattern (memory efficient)
func extractAndCopy(
	srcDir, destDir, volumeName string,
	factory *pb.PoolProgressFactory,
	fileCopyCallback FileCopier,
	maxNumBackups int,
	logFunc func(msg string),
	shutdownChan chan struct{},
	ignoreFunc func(path string) bool,
	bp *BackupProcessor,
) error {

	// First pass: scan source and populate BackupProcessor
	srcVisitor := func(file core.Fname, fm core.FileMetadata, fi os.FileInfo) error {
		if core.IsMetadataFile(string(file)) {
			return nil
		}
		if fm.HasTag(volumeName) {
			return nil
		}
		lenArchive := len(fm.BackupDestinations())
		if lenArchive > maxNumBackups {
			return nil
		}

		// Check if file should be ignored
		fp := fm.Path()
		if ignoreFunc != nil && ignoreFunc(fp.String()) {
			return nil
		}

		if shutdownChan != nil {
			select {
			case <-shutdownChan:
				return fmt.Errorf("shutdown requested")
			default:
			}
		}
		checksum := fm.GetChecksum()

		if dstPath, exists := bp.checkDstFileExists(checksum); exists {
			if err := bp.markAsMatched(checksum); err != nil {
				return err
			}
			_, err := updateSourceDirectoryMap(fp.Dir(), core.Fname(fp.Base()), volumeName, fm.Path())
			if err != nil {
				return err
			}
			_ = dstPath
			return nil
		}

		return bp.addSrcFile(checksum, fm.GetSize(), fm.BackupDestinations(), fp)
	}
	logFunc("Scanning source for files to backup")
	dwSrc := core.NewProgressableDirectoryWalker(core.MakeTokenChan(4), srcDir)
	if factory != nil {
		if err := factory.Register(dwSrc.Progress); err != nil {
			log.Printf("failed to register source progress: %v", err)
		}
	}

	dwSrc.AddFileVisitor(srcVisitor)

	if err := dwSrc.Walk(srcDir); err != nil {
		return fmt.Errorf("error scanning source: %w", err)
	}
	logFunc("Completed source scan")

	return nil
}

// copyPendingFiles copies files queued in BackupProcessor after all sources are scanned.
// This runs after orphan deletion to ensure space reclamation happens first.
func copyPendingFiles(
	srcDirs []string,
	destDir, volumeName string,
	fileCopyCallback FileCopier,
	logFunc func(string),
	ignoreFunc func(path string) bool,
	bp *BackupProcessor,
) error {
	if fileCopyCallback == nil {
		return nil
	}

	logFunc("Starting file copy")
	next, _ := bp.prioritizedSrcFiles()
	for fp, ok := next(); ok; fp, ok = next() {
		// Determine which source root this file belongs to
		srcRoot, found := findSourceRoot(srcDirs, fp.String())
		if !found {
			return fmt.Errorf("unable to determine source root for %s", fp)
		}

		if ignoreFunc != nil && ignoreFunc(fp.String()) {
			continue
		}

		logFunc(fmt.Sprintf("Copying file: %s", fp))
		if err := doACopy(srcRoot, destDir, volumeName, fp, fileCopyCallback); err != nil {
			if errors.Is(err, ErrNoSpace) {
				return ErrNoSpace
			}
			return err
		}

		rel, err := filepath.Rel(srcRoot, fp.String())
		if err != nil {
			return err
		}
		dir := core.Dirname(filepath.Dir(fp.String()))
		fn := core.Fname(filepath.Base(fp.String()))
		dmSrc, err := core.DirectoryMapFromDir(dir)
		if err != nil {
			return err
		}
		fs, ok := dmSrc.Get(core.Fname(fn))
		if !ok {
			// Try to rebuild the directory entry for the missing file
			log.Printf("Missing directory entry for %s in %s, attempting to rebuild", fn, dir)

			// Create a new FileStruct from the actual file
			fs, err = core.NewFileStruct(string(dir), string(fn))
			if err != nil {
				return fmt.Errorf("failed to create directory entry for %s: %w", fp, err)
			}

			// Calculate checksum if not present
			if fs.Checksum == "" {
				cks, err := core.CalcMd5File(string(dir), string(fn))
				if err != nil {
					return fmt.Errorf("failed to calculate checksum for %s: %w", fp, err)
				}
				fs.Checksum = cks
			}

			// Add the rebuilt entry to the directory map
			dmSrc.Add(fs)

			// Persist the updated directory map
			if err := dmSrc.Persist(dir); err != nil {
				log.Printf("warning: failed to persist rebuilt directory entry for %s: %v", fp, err)
			}
		}
		dstPath := core.NewFpath(destDir, rel)
		if err := bp.markAsMatched(fs.Checksum); err != nil {
			return err
		}
		if err := bp.addDstFile(fs.Checksum, fs.Size, fs.BackupDest, dstPath); err != nil {
			return err
		}
	}

	logFunc("Completed file copy")
	return nil
}

// findSourceRoot finds the matching source root for a file path.
func findSourceRoot(srcDirs []string, filePath string) (string, bool) {
	for _, root := range srcDirs {
		if root == "" {
			continue
		}
		rel, err := filepath.Rel(root, filePath)
		if err != nil {
			continue
		}
		if rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))) {
			return root, true
		}
	}
	return "", false
}

func tagSourceAsBackedUp(file core.Fpath, backupLabelName string) (core.FileStruct, error) {
	basename := filepath.Base(file.String())
	sd := file.Dir()
	return updateSourceDirectoryMap(sd, core.Fname(basename), backupLabelName, file)
}

func updateSourceDirectoryMap(dir core.Dirname, filename core.Fname, backupLabelName string, srcFile core.Fpath) (core.FileStruct, error) {
	srcLock := globalDirLocks.getLock(string(dir))
	srcLock.Lock()
	defer srcLock.Unlock()

	dmSrc, err := core.DirectoryMapFromDir(dir)
	if err != nil {
		return core.FileStruct{}, err
	}
	src, ok := dmSrc.Get(core.Fname(filename))
	if !ok {
		// Try to rebuild the directory entry for the missing file
		log.Printf("Missing directory entry for %s in %s, attempting to rebuild", filename, dir)

		// Create a new FileStruct from the actual file
		src, err = core.NewFileStruct(string(dir), string(filename))
		if err != nil {
			return core.FileStruct{}, fmt.Errorf("failed to create directory entry for %s: %w", srcFile, err)
		}

		// Calculate checksum if not present
		if src.Checksum == "" {
			cks, err := core.CalcMd5File(string(dir), string(filename))
			if err != nil {
				return core.FileStruct{}, fmt.Errorf("failed to calculate checksum for %s: %w", srcFile, err)
			}
			src.Checksum = cks
		}

		// Add the rebuilt entry to the directory map
		dmSrc.Add(src)

		// Persist the updated directory map
		if err := dmSrc.Persist(dir); err != nil {
			log.Printf("warning: failed to persist rebuilt directory entry for %s: %v", srcFile, err)
		}
	}
	if src.HasTag(backupLabelName) {
		return src, nil
	}
	if added := src.AddTag(backupLabelName); !added {
		return src, fmt.Errorf("failed to add tag %s", backupLabelName)
	}
	dmSrc.Add(src)
	return src, dmSrc.Persist(dir)
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
	rel, err := filepath.Rel(srcDir, file.String())
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
	src, err := tagSourceAsBackedUp(file, backupLabelName)
	if err != nil {
		return err
	}

	// _ = src.RemoveTag(backupLabelName)
	// Update the destDir with the checksum from the srcDir

	// Acquire lock for destination directory
	destDirForLock := core.Dirname(filepath.Join(destDir, filepath.Dir(rel)))
	dstLock := globalDirLocks.getLock(string(destDirForLock))
	dstLock.Lock()
	defer dstLock.Unlock()

	dmDst, err := core.DirectoryMapFromDir(destDirForLock)
	if err != nil {
		return err
	}
	fs, err := os.Stat(filepath.Join(destDir, rel))
	if err != nil {
		return err
	}
	src.SetDirectory(destDirForLock)
	src.Mtime = fs.ModTime().Unix()
	dmDst.Add(src)
	return dmDst.Persist(destDirForLock)
}

// BackupRunner handles backup from one or more source directories to a single destination.
// Supports single or multiple sources with proper orphan detection across all sources.
// ignoreFunc is optional and should return true if a path should be ignored.
func BackupRunner(
	volumeLabel VolumeLabeler,
	maxNumBackups int,
	fileCopyCallback FileCopier,
	destDir string,
	orphanFunc func(path string) error,
	logFunc func(msg string),
	factory *pb.PoolProgressFactory,
	shutdownChan chan struct{},
	skipCheckCalc bool,
	ignoreFunc func(path string) bool,
	srcDirs ...string,
) error {
	if len(srcDirs) == 0 {
		return fmt.Errorf("at least one source directory required")
	}
	if logFunc == nil {
		logFunc = func(msg string) {
			log.Println(msg)
		}
	}
	bp, err := NewBackupProcessor()
	if err != nil {
		return err
	}
	defer bp.Close()
	backupLabelName, err := volumeLabel.GetVolumeLabel(destDir)
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
	if !skipCheckCalc {
		logFunc("Running mdcalc on all directories (destination + sources)")
		if err := RunCheckCalc(allDirs, checkCalcOpts); err != nil {
			return fmt.Errorf("error running mdcalc: %w", err)
		}
		logFunc("Finished mdcalc")

	} else {
		logFunc("Skipping mdcalc (using existing checksums)")
	}

	// Step 2: Scan destination directory with streaming visitor (no memory accumulation)
	logFunc("Initial scan for destination inventory")
	// Use ProgressableDirectoryWalker to track progress
	dwDest := core.NewProgressableDirectoryWalker(core.MakeTokenChan(4), destDir)
	if factory != nil {
		if err := factory.Register(dwDest.Progress); err != nil {
			log.Printf("failed to register destination progress: %v", err)
		}
	}

	dwDest.AddFileVisitor(func(file core.Fname, fm core.FileMetadata, fi os.FileInfo) error {
		if core.IsMetadataFile(string(file)) {
			return nil
		}
		if shutdownChan != nil {
			select {
			case <-shutdownChan:
				return filepath.SkipAll
			default:
			}
		}

		checksum := fm.GetChecksum()

		return bp.addDstFile(checksum, fm.GetSize(), fm.BackupDestinations(), fm.Path())
	})

	if err := dwDest.Walk(destDir); err != nil {
		return fmt.Errorf("error scanning destination: %w", err)
	}

	if fileCopyCallback == nil {
		logFunc("Scan only requested; performing ingestion without copying")
	}

	// Phase 2: scan all sources to populate backup plan (no copying yet)
	for i, srcDir := range srcDirs {
		logFunc(fmt.Sprintf("Scanning source %d for files to backup", i+1))
		if err := extractAndCopy(
			srcDir, destDir, backupLabelName,
			factory,
			nil, // scan only
			maxNumBackups,
			logFunc,
			shutdownChan,
			ignoreFunc,
			bp,
		); err != nil {
			return fmt.Errorf("BackupRunner failed scanning source %d, %w", i+1, err)
		}
	}

	// Phase 3: delete orphans now that all sources are known
	if orphanFunc != nil {
		logFunc("Reporting orphaned destination files")
		for _, orphan := range bp.getOrphanFiles() {
			if err := orphanFunc(orphan.String()); err != nil {
				return err
			}
		}
	}

	// Phase 4: perform copies using the populated plan
	if err := copyPendingFiles(srcDirs, destDir, backupLabelName, fileCopyCallback, logFunc, ignoreFunc, bp); err != nil {
		return err
	}

	return nil
}
