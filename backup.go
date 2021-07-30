package medorg

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// MaxBackups ifs the maximum number of drives we will backup a file to
var MaxBackups = 4

// ErrMissingEntry You are copying a file that there is no directory entry for. Probably need to rerun a visit on the directory
var ErrMissingEntry = errors.New("attempting to copy a file there seems to be no directory entry for")

// errMissingSrcEntry should only happen if there is an internal logic error
var errMissingSrcEntry = errors.New("missing source entry")

// errMissingCopyEntry internal error
var errMissingCopyEntry = errors.New("copying a file without an entry")

// ErrDummyCopy Return this from your copy function to skip the effects of copying on the md5 files
var ErrDummyCopy = errors.New("not really copying, it's all good though")

// Export of the generic IO error from syscall
var ErrIOError = syscall.Errno(5) // I don't like this, but don't know a better way
// Export of no space left on device from syscall
var ErrNoSpace = syscall.Errno(28)

type backupKey struct {
	size     int64
	checksum string
}
type backupDupeMap struct {
	sync.Mutex
	dupeMap map[backupKey]Fpath
}

// Add an entry to the map
func (bdm *backupDupeMap) Add(fs FileStruct) {
	key := backupKey{fs.Size, fs.Checksum}
	bdm.Lock()
	if bdm.dupeMap == nil {
		bdm.dupeMap = make(map[backupKey]Fpath)
	}
	bdm.dupeMap[key] = Fpath(fs.Path())
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
func (bdm *backupDupeMap) Get(key backupKey) (Fpath, bool) {
	if bdm.dupeMap == nil {
		return "", false
	}
	bdm.Lock()
	defer bdm.Unlock()
	v, ok := bdm.dupeMap[key]
	return v, ok
}

type backupMaker struct {
	visitFunc func(dm DirectoryMap, dir, fn string, d fs.DirEntry) error
}

func (bm backupMaker) backMake(dir string) (DirectoryTrackerInterface, error) {
	mkFk := func(dir string) (DirectoryEntryInterface, error) {
		// Not an issue if we get errors - we'll just have a blank dm and rebuild
		dm, _ := DirectoryMapFromDir(dir)
		dm.VisitFunc = bm.visitFunc
		return dm, nil
	}
	return NewDirectoryEntry(dir, mkFk)
}

type backScanner struct {
	dupeFunc   func(path string) error
	lookupFunc func(Fpath, bool) error
}

func (dm DirectoryMap) updateAndGo(dir, fn string) (fs FileStruct, err error) {
	// Update the checksum, creating the FS if needed
	err = dm.UpdateChecksum(dir, fn, false)
	if err != nil {
		return
	}

	// Add everything we find to the destination map
	fs, ok := dm.Get(fn)
	if Debug && !ok {
		// If the FS does not exist, then UpdateChecksum is faulty
		return fs, fmt.Errorf("dst %w: %s/%s", errMissingSrcEntry, dir, fn)
	}
	return
}

// scanBackupDirectories will mark srcDir's ArchiveAt
// tag, with any files that are already found in the destination
func (bs backScanner) scanBackupDirectories(destDir, srcDir, volumeName string, logFunc func(msg string),
	registerFunc func(*DirTracker),
) error {
	if logFunc == nil {
		logFunc = func(msg string) {
		}
	}
	calcCnt := 2
	tokenBuffer := makeTokenChan(calcCnt)
	defer close(tokenBuffer)

	var backupDestination backupDupeMap
	var backupSource backupDupeMap

	modifyFuncDestinationDm := func(dm DirectoryMap, dir, fn string, d fs.DirEntry) error {
		logFunc("Examining Destination")
		if fn == Md5FileName {
			return nil
		}
		<-tokenBuffer
		fs, err := dm.updateAndGo(dir, fn)
		tokenBuffer <- struct{}{}

		if err != nil {
			return err
		}
		backupDestination.Add(fs)
		return nil
	}
	modifyFuncSourceDm := func(dm DirectoryMap, dir, fn string, d fs.DirEntry) error {
		logFunc("Examining Source")
		if fn == Md5FileName {
			return nil
		}
		<-tokenBuffer
		fs, err := dm.updateAndGo(dir, fn)
		tokenBuffer <- struct{}{}
		if err != nil {
			return err
		}
		// If it exists in the destination already
		path, ok := backupDestination.Get(fs.Key())
		if bs.lookupFunc != nil {
			err = bs.lookupFunc(path, ok)
			if err != nil {
				return err
			}
		}
		if ok {
			// Then mark in the source as already backed up
			_ = fs.AddTag(volumeName)
		}
		if !ok && fs.HasTag(volumeName) {
			// FIXME add testcase for this
			// The case where the file is not present at the dest
			// but the tag says that it is
			fs.RemoveTag(volumeName)
		}

		backupSource.Add(fs)
		dm.Add(fs)
		return nil
	}

	makerFuncDest := backupMaker{
		visitFunc: modifyFuncDestinationDm,
	}
	makerFuncSrc := backupMaker{
		visitFunc: modifyFuncSourceDm,
	}
	errChan := runSerialDirTrackerJob([]dirTrackerJob{
		{destDir, makerFuncDest.backMake},
		{srcDir, makerFuncSrc.backMake},
	}, registerFunc)

	for err := range errChan {
		if err != nil {
			for range errChan {
			}
			return err
		}
	}

	if (bs.dupeFunc != nil) && (backupDestination.Len() > 0) {
		// There's stuff on the backup that's not in the Source
		// We'll need to do something about this soon!
		// log.Println("Unexpected items left in backup destination")
		for _, v := range backupDestination.dupeMap {
			bs.dupeFunc(string(v))
		}
	}
	return nil
}

// extractCopyFiles will look for files that are not backed up
// i.e. walk through src file system looking for files
// That don't have the volume name as an archived at
func extractCopyFiles(targetDir, volumeName string, registerFunc func(*DirTracker)) (fpathListList, error) {
	var lk sync.Mutex
	remainingFiles := fpathListList{}
	visitFunc := func(dm DirectoryMap, dir, fn string, d fs.DirEntry) error {
		if fn == Md5FileName {
			return nil
		}
		fs, ok := dm.Get(fn)
		if !ok {
			return fmt.Errorf("%w: %s/%s", errMissingCopyEntry, dir, fn)
		}
		if fs.HasTag(volumeName) {
			return nil
		}
		fp := NewFpath(dir, fn)
		lenArchive := len(fs.ArchivedAt)
		if lenArchive > MaxBackups {
			return nil
		}
		lk.Lock()
		remainingFiles.Add(lenArchive, fp)
		lk.Unlock()
		return nil
	}

	makerFunc := func(dir string) (DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (DirectoryEntryInterface, error) {
			dm, err := DirectoryMapFromDir(dir)
			dm.VisitFunc = visitFunc
			return dm, err
		}
		return NewDirectoryEntry(dir, mkFk)
	}
	ndt := NewDirTracker(targetDir, makerFunc)
	if registerFunc != nil {
		registerFunc(ndt)
	}
	errChan := ndt.ErrChan()
	for err := range errChan {
		for range errChan {
		}
		if err != nil {
			return remainingFiles, fmt.Errorf("extractCopyFiles::%w", err)
		}
	}
	return remainingFiles, nil
}

type FileCopier func(src, dst Fpath) error

func doACopy(
	srcDir, // The source of the backup as specified on the command line
	destDir, // The destination directory as specified...
	backupLabelName string, // the tag w should add to the sorce
	file Fpath, // The full path of the file
	fc FileCopier) error {
	if fc == nil {
		fc = CopyFile
	}

	// Workout the new path the target file should have
	// this is relative to the srcdir so that
	// the dst dir keeps the hierarchy
	rel, err := filepath.Rel(srcDir, string(file))
	if err != nil {
		return err
	}

	// Actually copy the file
	err = fc(file, NewFpath(destDir, rel))
	if errors.Is(err, ErrDummyCopy) {
		return nil
	}
	if errors.Is(err, ErrNoSpace) {
		_ = rmFilename(NewFpath(destDir, rel))
		return ErrNoSpace
	}
	// Update the srcDir .md5 file with the fact we've backed this up now
	basename := filepath.Base(string(file))
	sd := filepath.Dir(string(file))
	if err != nil {
		return err
	}
	dmSrc, err := DirectoryMapFromDir(sd)
	if err != nil {
		return err
	}
	src, ok := dmSrc.Get(basename)
	if !ok {
		return fmt.Errorf("%w: %s, \"%s\" \"%s\"", ErrMissingEntry, file, sd, basename)
	}
	_ = src.AddTag(backupLabelName)
	dmSrc.Add(src)
	dmSrc.Persist(srcDir)
	_ = src.RemoveTag(backupLabelName)
	// Update the destDir with the checksum from the srcDir
	dmDst, err := DirectoryMapFromDir(destDir)
	if err != nil {
		return err
	}
	fs, err := os.Stat(filepath.Join(destDir, rel))
	if err != nil {
		return err
	}
	src.directory = destDir
	src.Mtime = fs.ModTime().Unix()
	dmDst.Add(src)
	dmDst.Persist(destDir)
	return nil
}

func BackupRunner(
	xc *XMLCfg,
	maxNumBackups int,
	fc FileCopier,
	srcDir, destDir string,
	orphanFunc func(path string) error,
	logFunc func(msg string),
	registerFunc func(*DirTracker),
) error {

	if logFunc == nil {
		logFunc = func(msg string) {
			log.Println(msg)
		}
	}
	backupLabelName, err := xc.getVolumeLabel(destDir)
	if err != nil {
		return err
	}
	logFunc(fmt.Sprint("Determined label as:", backupLabelName, "now scanning directories"))

	// Go ahead and run a check_calc style scan of the directories and make sure
	// they have all their existing md5s up to date
	// First of all get the srcDir updated with files that are already in destDir
	var bs backScanner
	err = bs.scanBackupDirectories(destDir, srcDir, backupLabelName, logFunc, registerFunc)
	if err != nil {
		return err
	}
	if fc == nil {
		logFunc("Scan only. Going no further")
		// If we've not supplied a copier, when we clearly don't want to run the copy
		return nil
	}
	logFunc("Looking for files to  copy")
	copyFilesArray, err := extractCopyFiles(srcDir, backupLabelName, registerFunc)
	if err != nil {
		return fmt.Errorf("BackupRunner cannot extract files, %w", err)
	}

	// FIXME Now run this through Prioritize
	logFunc("Now starting Copy")
	// Now do the copy, updating srcDir's labels as we go
	for numBackups, copyFiles := range copyFilesArray {
		if numBackups >= maxNumBackups {
			logFunc(fmt.Sprint("Not backing up to more than", maxNumBackups, "places"))
			return nil
		}
		for _, file := range copyFiles {
			err := doACopy(srcDir, destDir, backupLabelName, file, fc)
			if errors.Is(err, ErrNoSpace) {
				// FIXME in the ideal world, we'd look at how much space there is left on the volume
				// and look for a file with a size smaller than that
				// and copy that.
				// For now, that optimization is not too bad.
				logFunc("Destination full")
				return nil
			}
			if err != nil {
				return fmt.Errorf("copy failed, %w::%s, %s, %s, %s", err, srcDir, destDir, backupLabelName, file)
			}
		}
	}
	logFunc("Finished Copy")
	return nil
}
