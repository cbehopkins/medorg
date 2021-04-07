package medorg

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
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

func (bdm *backupDupeMap) Len() int {
	bdm.Lock()
	defer bdm.Unlock()
	return len(bdm.dupeMap)

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
func scanBackupDirectories(destDir, srcDir, volumeName string) {
	backupDestination := NewBackupDupeMap()
	backupSource := NewBackupDupeMap()
	modifyFuncDestination := func(de DirectoryEntry, dir, fn string, d fs.DirEntry) error {
		if strings.HasPrefix(fn, ".") {
			return nil
		}
		de.UpdateChecksum(fn, false)
		// Add everything we find to the destination map
		fs, ok := de.dm.Get(fn)
		if !ok {
			return fmt.Errorf("%w: %s/%s", ErrMissingSrcEntry, dir, fn)
		}
		backupDestination.Add(fs)
		return nil
	}
	modifyFuncSource := func(de DirectoryEntry, dir, fn string, d fs.DirEntry) error {
		if strings.HasPrefix(fn, ".") {
			return nil
		}
		de.UpdateChecksum(fn, false)
		fs, ok := de.dm.Get(fn)
		if !ok {
			return fmt.Errorf("%w: %s/%s", ErrMissingSrcEntry, dir, fn)
		}
		key := backupKey{fs.Size, fs.Checksum}
		// If it exists in the destination already
		_, ok = backupDestination.Lookup(key)
		if ok {
			// Then mark in the source as already backed up
			_ = fs.AddTag(volumeName)
			backupDestination.Remove(key)
		}
		if !ok && fs.HasTag(volumeName) {
			// FIXME add testcase for this
			fs.RemoveTag(volumeName)
		}

		backupSource.Add(fs)
		fs.Analysed = time.Now().Unix()
		de.SetFs(fs)
		return nil
	}

	makerFuncDest := func(dir string) DirectoryTrackerInterface {
		return NewDirectoryEntry(dir, modifyFuncDestination)
	}
	makerFuncSrc := func(dir string) DirectoryTrackerInterface {
		return NewDirectoryEntry(dir, modifyFuncSource)
	}
	// FIXME we should be able to run this in parallel
	for err := range NewDirTracker(destDir, makerFuncDest) {
		fmt.Println("Error received on closing:", err)
	}
	for err := range NewDirTracker(srcDir, makerFuncSrc) {
		fmt.Println("Error received on closing:", err)
	}
	if backupDestination.Len() > 0 {
		// There's stuff on the backup that's not in the Source
		// We'll need to do somethign about this soon!
		log.Println("Unexpected items left in backup destination")
		for _, v := range backupDestination.dupeMap {
			log.Println(v)
		}
	}
}

// extractCopyFiles will look for files that are not backed up
func extractCopyFiles(targetDir, volumeName string) fpathListList {
	remainingFiles := fpathListList{}
	modifyFunc := func(de DirectoryEntry, dir, fn string, d fs.DirEntry) error {
		if strings.HasPrefix(fn, ".") {
			return nil
		}
		fs, ok := de.dm.Get(fn)
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

	makerFunc := func(dir string) DirectoryTrackerInterface {
		return NewDirectoryEntry(dir, modifyFunc)
	}
	// FIXME we should be able to run this in parallel
	for err := range NewDirTracker(targetDir, makerFunc) {
		fmt.Println("Error received on closing:", err)
	}
	return remainingFiles
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
	dmSrc := DirectoryMapFromDir(srcDir)
	src, ok := dmSrc.Get(rel)
	if !ok {
		return fmt.Errorf("%w: %s", ErrMissingEntry, file)
	}
	_ = src.AddTag(backupLabelName)
	dmSrc.Add(src)
	dmSrc.WriteDirectory(srcDir)
	_ = src.RemoveTag(backupLabelName)
	// Update the destDir with the checksum from the srcDir
	dmDst := DirectoryMapFromDir(destDir)
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
	scanBackupDirectories(destDir, srcDir, backupLabelName)
	copyFilesArray := extractCopyFiles(srcDir, backupLabelName)
	// Now run this through Prioritize
	log.Println("Now starting Copy")
	// Now do the copy, updating srcDir's labels as we go
	for _, copyFiles := range copyFilesArray {
		for _, file := range copyFiles {
			err := doACopy(srcDir, destDir, backupLabelName, file, fc)
			// TBD catch destination full error
			if err != nil {
				log.Println("Received Error", err)
				return err
			}
		}
	}
	log.Println("Finished Copy")

	return nil
}
