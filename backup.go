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

var MaxBackups = 4

var ErrMissingEntry = errors.New("attempting to copy a file there seems to be no directory entry for")
var ErrMissingSrcEntry = errors.New("missing source entry")
var ErrMissingCopyEntry = errors.New("copying a file without an entry")
var ErrDummyCopy = errors.New("not really copying, it's all good though")
var ErrIOError = syscall.Errno(5) // I don't like this, but don't know a better way
var ErrNoSpace = syscall.Errno(28)

type backupKey struct {
	size     int64
	checksum string
}
type backupDupeMap struct {
	sync.Mutex
	dupeMap map[backupKey]Fpath
}

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
func (bdm *backupDupeMap) Remove(key backupKey) {
	if bdm.dupeMap == nil {
		return
	}
	bdm.Lock()
	delete(bdm.dupeMap, key)
	bdm.Unlock()
}

func (bdm *backupDupeMap) Lookup(key backupKey) (Fpath, bool) {
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
		dm, err := DirectoryMapFromDir(dir)
		dm.VisitFunc = bm.visitFunc
		return dm, err
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
		return fs, fmt.Errorf("dst %w: %s/%s", ErrMissingSrcEntry, dir, fn)
	}
	return
}

// scanBackupDirectories will mark srcDir's ArchiveAt
// tag, with any files that are already found in the destination
func (bs backScanner) scanBackupDirectories(destDir, srcDir, volumeName string) error {
	calcCnt := 2
	tokenBuffer := makeTokenChan(calcCnt)
	defer close(tokenBuffer)

	var backupDestination backupDupeMap
	var backupSource backupDupeMap

	modifyFuncDestinationDm := func(dm DirectoryMap, dir, fn string, d fs.DirEntry) error {
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
		path, ok := backupDestination.Lookup(fs.Key())
		if bs.lookupFunc != nil {
			err = bs.lookupFunc(path, ok)
			if err != nil {
				return err
			}
		}
		if ok {
			// Then mark in the source as already backed up
			_ = fs.AddTag(volumeName)
			backupDestination.Remove(fs.Key())
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
	})

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
func extractCopyFiles(targetDir, volumeName string) (fpathListList, error) {
	var lk sync.Mutex
	remainingFiles := fpathListList{}
	visitFunc := func(dm DirectoryMap, dir, fn string, d fs.DirEntry) error {
		if fn == Md5FileName {
			return nil
		}
		fs, ok := dm.Get(fn)
		if !ok {
			return fmt.Errorf("%w: %s/%s", ErrMissingCopyEntry, dir, fn)
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
	errChan := NewDirTracker(targetDir, makerFunc)
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

func BackupRunner(xc *XMLCfg, fc FileCopier, srcDir, destDir string, orphanFunc func(path string) error) error {
	// Go ahead and run a check_calc style scan of the directories and make sure
	// they have all their existing md5s up to date
	backupLabelName, err := getVolumeLabel(xc, destDir)
	if err != nil {
		return err
	}
	log.Println("Determined label as:", backupLabelName)
	// First of all get the srcDir updated with files that are already in destDir
	var bs backScanner
	err = bs.scanBackupDirectories(destDir, srcDir, backupLabelName)
	if err != nil {
		return err
	}
	if fc == nil {
		log.Println("Scan only. Going no further")
		// If we've not supplied a copier, when we clearly don't want to run the copy
		return nil
	}
	copyFilesArray, err := extractCopyFiles(srcDir, backupLabelName)
	if err != nil {
		return err
	}
	// FIXME Now run this through Prioritize
	log.Println("Now starting Copy")
	// Now do the copy, updating srcDir's labels as we go
	for _, copyFiles := range copyFilesArray {
		for _, file := range copyFiles {
			err := doACopy(srcDir, destDir, backupLabelName, file, fc)
			if errors.Is(err, ErrNoSpace) {
				log.Println("Destination full")
				return nil
			}
			if err != nil {
				return fmt.Errorf("copy failed, %w::%s, %s, %s, %s", err, srcDir, destDir, backupLabelName, file)
			}
		}
	}
	log.Println("Finished Copy")

	return nil
}
