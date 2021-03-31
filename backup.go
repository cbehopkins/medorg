package medorg

import "time"

type backupKey struct {
	size     int64
	checksum string
}
type backupDupeMap map[backupKey]fpath

func (bdm backupDupeMap) add(fs FileStruct) {
	key := backupKey{fs.Size, fs.Checksum}
	bdm[key] = fpath(fs.Path())
}

func (bdm0 backupDupeMap) findDuplicates(bdm1 backupDupeMap) <-chan []fpath {
	matchChan := make(chan []fpath)
	go func() {
		for key, value := range bdm0 {
			val, ok := bdm1[key]
			if ok {
				matchChan <- []fpath{value, val}
			}
		}
		close(matchChan)
	}()
	return matchChan
}

// scanBackupDirectories will mark srcDir's ArchiveAt
// tag, with any files that are already found in the destination
func scanBackupDirectories(destDir, srcDir, volumeName string) {
	tu := NewTreeUpdate(1, 1, 1)
	backupDestination := make(backupDupeMap)
	backupSource := make(backupDupeMap)

	modifyFuncDestination := func(dir, fn string, fs FileStruct) (FileStruct, bool) {
		// Add everything we find to the destination map
		backupDestination.add(fs)
		return fs, false
	}
	modifyFuncSource := func(dir, fn string, fs FileStruct) (FileStruct, bool) {
		key := backupKey{fs.Size, fs.Checksum}
		// If it exists in the destination already
		_, ok := backupDestination[key]
		if ok {
			// Then mark in the source as already backed up here
			_ = fs.AddTag(volumeName)
		}
		backupSource.add(fs)
		fs.Analysed = time.Now().Unix()

		return fs, true
	}
	tu.UpdateDirectory(destDir, modifyFuncDestination)
	tu.UpdateDirectory(srcDir, modifyFuncSource)
}

type fpathList []fpath

func (fpl *fpathList) Add(fp fpath) {
	*fpl = append(*fpl, fp)
}

type fpathListList []fpathList

func (fpll *fpathListList) Add(index int, fp fpath) {
	// FIXME there's an inefficiency here, that we (should) never use index 0
	for len(*fpll) <= index {
		// append until we have a list that is long enough
		// TBD potentially add several in one go
		*fpll = append(*fpll, fpathList{})
	}
	toAdd := *fpll
	
	toAdd[index].Add(fp)
}

// extractCopyFiles will look for files that are not backed up
func extractCopyFiles(targetDir, volumeName string) <-chan fpath {
	resultChan := make(chan fpath)
	remainingFiles := fpathListList{}
	go func() {
		defer close(resultChan)
		tu := NewTreeUpdate(1, 1, 1)
		modifyFunc := func(dir, fn string, fs FileStruct) (FileStruct, bool) {
			fp := Fpath(dir, fn)
			lenArchive := len(fs.ArchivedAt)
			if lenArchive == 0 {
				resultChan <- fp
				return fs, false
			}
			if fs.HasTag(volumeName) {
				return fs, false
			}
			remainingFiles.Add(lenArchive, fp)

			return fs, false
		}
		tu.UpdateDirectory(targetDir, modifyFunc)
		for _, value := range remainingFiles {
			for _, v := range value {
				resultChan <- v
			}
		}
	}()
	return resultChan
}
