package medorg

import (
	"log"
	"path/filepath"
	"sync"
	"time"
)

type backupKey struct {
	size     int64
	checksum string
}
type backupDupeMap struct {
	sync.Mutex
	dupeMap map[backupKey]fpath
}

func NewBackupDupeMap() (itm backupDupeMap) {
	itm.dupeMap = make(map[backupKey]fpath)
	return
}
func (bdm *backupDupeMap) Add(fs FileStruct) {
	key := backupKey{fs.Size, fs.Checksum}
	bdm.Lock()
	bdm.dupeMap[key] = fpath(fs.Path())
	bdm.Unlock()
}
func (bdm *backupDupeMap) Lookup(key backupKey) (fpath, bool) {
	bdm.Lock()
	defer bdm.Unlock()
	v, ok := bdm.dupeMap[key]
	return v, ok
}
func (bdm0 backupDupeMap) findDuplicates(bdm1 backupDupeMap) <-chan []fpath {
	matchChan := make(chan []fpath)
	go func() {
		bdm0.Lock()
		bdm1.Lock()
		for key, value := range bdm0.dupeMap {
			val, ok := bdm1.dupeMap[key]
			if ok {
				// Value found in both maps
				matchChan <- []fpath{value, val}
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
	tu := NewTreeUpdate(1, 1, 1)
	backupDestination := NewBackupDupeMap()
	backupSource := NewBackupDupeMap()

	modifyFuncDestination := func(dir, fn string, fs FileStruct) (FileStruct, bool) {
		// Add everything we find to the destination map
		backupDestination.Add(fs)
		return fs, false
	}
	modifyFuncSource := func(dir, fn string, fs FileStruct) (FileStruct, bool) {
		key := backupKey{fs.Size, fs.Checksum}
		// If it exists in the destination already
		_, ok := backupDestination.Lookup(key)
		if ok {
			// Then mark in the source as already backed up here
			_ = fs.AddTag(volumeName)
		}
		backupSource.Add(fs)
		fs.Analysed = time.Now().Unix()

		return fs, true
	}
	tu.UpdateDirectory(destDir, modifyFuncDestination)
	tu.UpdateDirectory(srcDir, modifyFuncSource)
}

// extractCopyFiles will look for files that are not backed up
func extractCopyFiles(targetDir, volumeName string) fpathListList {
	remainingFiles := fpathListList{}
	tu := NewTreeUpdate(1, 1, 1)
	modifyFunc := func(dir, fn string, fs FileStruct) (FileStruct, bool) {
		fp := Fpath(dir, fn)
		lenArchive := len(fs.ArchivedAt)
		if fs.HasTag(volumeName) {
			return fs, false
		}
		// FIXME Add a "do not add if already backed up to >= n places"
		remainingFiles.Add(lenArchive, fp)

		return fs, false
	}
	tu.UpdateDirectory(targetDir, modifyFunc)
	return remainingFiles
}

type FileCopier func(src, dst fpath) error

func doACopy(srcDir, destDir string, file fpath, fc FileCopier) error {
	if fc == nil {
		fc = CopyFile
	}
	log.Println("Copy", file, srcDir, destDir)
	// Workout the new path the target file should have
	rel, err := filepath.Rel(srcDir, string(file))
	if err != nil {
		return err
	}

	// Actually copy the file
	fc(file, Fpath(destDir, rel))
	// Update the srcDir .md5 file with the fact we've backed this up now

	// Update the destDir with the checksum from the srcDir
	return nil
}

func BackupRunner(xc *XMLCfg, srcDir, destDir string, fc FileCopier) error {
	// Go ahead and run a check_calc style scan of the directories and make sure
	// they have all their existing md5s up to date
	backupLabelName, err := getVolumeLabel(xc, destDir)
	if err != nil {
		return err
	}
	// First of all get the srcDir updated with files that are already in destDir
	scanBackupDirectories(destDir, srcDir, backupLabelName)
	copyFilesArray := extractCopyFiles(srcDir, backupLabelName)
	// Now run this through Prioritize

	// Now do the copy, updating srcDir's labels as we go
	for _, copyFiles := range copyFilesArray {
		for _, file := range copyFiles {
			err := doACopy(srcDir, destDir, file, fc)
			// TBD catch destination full error
			if err != nil {
				return err
			}
		}
	}
	return nil
}
