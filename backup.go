package medorg
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