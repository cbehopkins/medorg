package medorg

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var MaxBackups = 4

var ErrMissingEntry = errors.New("attempting to copy a file there seems to be no directory entry for")
var ErrMissingSrcEntry = errors.New("missing source entry")
var ErrMissingCopyEntry = errors.New("copying a file without an entry")

type backupKey struct {
	size     int64
	checksum string
}
type backupDupeMap struct {
	sync.Mutex
	dupeMap map[backupKey]Fpath
}

func NewBackupDupeMap() (itm backupDupeMap) {
	itm.dupeMap = make(map[backupKey]Fpath)
	return
}
func (bdm *backupDupeMap) Add(fs FileStruct) {
	key := backupKey{fs.Size, fs.Checksum}
	bdm.Lock()
	bdm.dupeMap[key] = Fpath(fs.Path())
	bdm.Unlock()
}

func (bdm *backupDupeMap) Remove(key backupKey) {
	bdm.Lock()
	delete(bdm.dupeMap, key)
	bdm.Unlock()
}

func (bdm *backupDupeMap) Lookup(key backupKey) (Fpath, bool) {
	bdm.Lock()
	defer bdm.Unlock()
	v, ok := bdm.dupeMap[key]
	return v, ok
}
func (bdm0 *backupDupeMap) findDuplicates(bdm1 *backupDupeMap) <-chan []Fpath {
	matchChan := make(chan []Fpath)
	go func() {
		bdm0.Lock()
		bdm1.Lock()
		for key, value := range bdm0.dupeMap {
			val, ok := bdm1.dupeMap[key]
			if ok {
				// Value found in both maps
				matchChan <- []Fpath{value, val}
			}
		}
		bdm1.Unlock()
		bdm0.Unlock()
		close(matchChan)
	}()
	return matchChan
}

// scanBackupDirectories will mark srcDir's ArchiveAt
// tag, with any files that are already found in the destination
func scanBackupDirectories(destDir, srcDir, volumeName string) error {
	calcCnt := 2
	tokenBuffer := makeTokenChan(calcCnt)
	defer close(tokenBuffer)

	backupDestination := NewBackupDupeMap()
	backupSource := NewBackupDupeMap()
	modifyFuncDestinationDm := func(dm DirectoryMap, dir, fn string, d fs.DirEntry) error {

		if fn == Md5FileName {
			return nil
		}
		err := dm.UpdateChecksum(dir, fn, false)
		if err != nil {
			return err
		}
		// Add everything we find to the destination map
		fs, ok := dm.Get(fn)
		if !ok {
			return fmt.Errorf("dst %w: %s/%s", ErrMissingSrcEntry, dir, fn)
		}
		backupDestination.Add(fs)
		return nil
	}
	modifyFuncSourceDm := func(dm DirectoryMap, dir, fn string, d fs.DirEntry) error {
		if fn == Md5FileName {
			return nil
		}
		<-tokenBuffer
		err := dm.UpdateChecksum(dir, fn, false)
		tokenBuffer <- struct{}{}

		if err != nil {
			return err
		}

		fs, ok := dm.Get(fn)
		if !ok {
			return fmt.Errorf("src %w: %s/%s", ErrMissingSrcEntry, dir, fn)
		}
		// If it exists in the destination already
		_, ok = backupDestination.Lookup(fs.Key())
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
		fs.Analysed = time.Now().Unix()
		dm.Add(fs)
		return nil
	}
	makerFuncDest := func(dir string) (DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (DirectoryEntryInterface, error) {
			dm, err := DirectoryMapFromDir(dir)
			// fk := func(dir, fn string, d fs.DirEntry) error {
			// 	return modifyFuncDestinationDm(dm, dir, fn, d)
			// }
			dm.visitor = modifyFuncDestinationDm
			return dm, err
		}
		return NewDirectoryEntry(dir, nil, mkFk), nil
	}
	makerFuncSrc := func(dir string) (DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (DirectoryEntryInterface, error) {
			dm, err := DirectoryMapFromDir(dir)
			dm.visitor = modifyFuncSourceDm
			return dm, err

		}
		return NewDirectoryEntry(dir, nil, mkFk), nil
	}

	errChan := runSerialDirTrackerJob([]dirTrackerJob{
		{destDir, makerFuncDest},
		{srcDir, makerFuncSrc},
	})

	for err := range errChan {
		if err != nil {
			for range errChan {
			}
			return err
		}
	}

	//if backupDestination.Len() > 0 {
	// There's stuff on the backup that's not in the Source
	// We'll need to do somethign about this soon!
	// log.Println("Unexpected items left in backup destination")
	// for _, v := range backupDestination.dupeMap {
	// 	log.Println(v)
	// }
	//}
	return nil
}

// extractCopyFiles will look for files that are not backed up
// i.e. walk through src file system looking for files
// That don't have the volume name as an archived at
func extractCopyFiles(targetDir, volumeName string) (fpathListList, error) {
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
		remainingFiles.Add(lenArchive, fp)

		return nil
	}

	makerFunc := func(dir string) (DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (DirectoryEntryInterface, error) {
			dm, err := DirectoryMapFromDir(dir)
			dm.visitor = visitFunc
			return dm, err
		}
		return NewDirectoryEntry(dir, nil, mkFk), nil
	}
	errChan := NewDirTracker(targetDir, makerFunc)
	for err := range errChan {
		for range errChan {
		}
		errWrapped := fmt.Errorf("extractCopyFiles::%w", err)
		if errWrapped != nil {
			return remainingFiles, errWrapped
		}
	}
	return remainingFiles, nil
}

type FileCopier func(src, dst Fpath) error

func doACopy(srcDir, destDir, backupLabelName string, file Fpath, fc FileCopier) error {
	if fc == nil {
		fc = CopyFile
	}
	// log.Println("Copy", file, srcDir, destDir)
	// Workout the new path the target file should have
	rel, err := filepath.Rel(srcDir, string(file))
	if err != nil {
		return err
	}

	// Actually copy the file
	fc(file, NewFpath(destDir, rel))
	// Update the srcDir .md5 file with the fact we've backed this up now
	dmSrc, err := DirectoryMapFromDir(srcDir)
	if err != nil {
		return err
	}
	src, ok := dmSrc.Get(rel)
	if !ok {
		return fmt.Errorf("%w: %s", ErrMissingEntry, file)
	}
	_ = src.AddTag(backupLabelName)
	dmSrc.Add(src)
	dmSrc.WriteDirectory(srcDir)
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
	dmDst.WriteDirectory(destDir)
	return nil
}

func BackupRunner(xc *XMLCfg, fc FileCopier, srcDir, destDir string) error {
	// Go ahead and run a check_calc style scan of the directories and make sure
	// they have all their existing md5s up to date
	backupLabelName, err := getVolumeLabel(xc, destDir)
	if err != nil {
		return err
	}
	log.Println("Determined label as:", backupLabelName)
	// First of all get the srcDir updated with files that are already in destDir
	err = scanBackupDirectories(destDir, srcDir, backupLabelName)
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
			if err != nil {
				return fmt.Errorf("copy failed, %w::%s, %s, %s, %s", err, srcDir, destDir, backupLabelName, file)
			}
		}
	}
	log.Println("Finished Copy")

	return nil
}
