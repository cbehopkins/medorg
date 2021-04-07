package medorg

import (
	"io/ioutil"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type reffer struct {
	fn   string
	size int64
}

func (rf reffer) Key() string {
	return rf.fn + strconv.FormatInt(rf.size, 10)
}

type trackerMap struct {
	// Given a filename & size, map to the checksum
	tm map[string]string
	lk *sync.RWMutex
}

//newTrackerMap create a new tracker of file structs
func newTrackerMap() *trackerMap {
	tm := new(trackerMap)
	tm.tm = make(map[string]string)
	tm.lk = new(sync.RWMutex)
	return tm
}
func (tm trackerMap) setChecksum(keyer reffer, checksum string) {
	tm.lk.Lock()
	tm.tm[keyer.Key()] = checksum
	tm.lk.Unlock()
}
func (tm *trackerMap) getChecksum(keyer reffer) (string, bool) {
	tm.lk.RLock()
	defer tm.lk.RUnlock()
	cSum, ok := tm.tm[keyer.Key()]
	return cSum, ok
}

// TreeWalker walks througha  directory tree
type TreeWalker struct {
	buildComplete bool
}

// NewTreeWalker creates a tree walker to control the walking of a directory
func NewTreeWalker() *TreeWalker {
	itm := new(TreeWalker)
	return itm
}

// SetBuildComplete marks the directory xml complete
// disablesd internal error checking
func (tw *TreeWalker) SetBuildComplete() {
	tw.buildComplete = true
}

// WalkFunc can modify the dm it is passed
// if one does this, you must return true
type WalkFunc func(directory, fn string, fs FileStruct, dm *DirectoryMap) bool

// DirectFunc is called at the end of walking each directory
type DirectFunc func(directory string, dm *DirectoryMap)

// WalkTree wlak through a directory tree, running the specified
// walk func is the func to run on the file walked
// direct func is run every directory and should call itself recursively
// FIXME why do we also have checksum.go's walkDirectory & tree_update's UpdateDirectory ?
func (tw TreeWalker) WalkTree(directory string, wf WalkFunc, df DirectFunc) {
	dm := DirectoryMapFromDir(directory)
	// Now read in all files in the current directory
	stats, err := ioutil.ReadDir(directory)
	if err != nil {
		log.Fatal(err)
	}
	var update bool
	for _, file := range stats {
		fn := file.Name()

		if strings.HasPrefix(fn, ".") {
			// Don't build for hidden files
			continue
		}
		// If it is a directory, then go into it
		if file.IsDir() {
			nd := filepath.Join(directory, fn)
			log.Println("Going into walk directory:", nd)
			tw.WalkTree(nd, wf, df)
			if Debug {
				log.Println("Finished with directory:", nd)
			}
		} else {
			var fs FileStruct
			fs, ok := dm.Get(fn)

			if ok {
				//log.Println("Found File", fn)
				if wf != nil {
					update = wf(directory, fn, fs, &dm) || update
				}
			}
		}
	}
	if update {
		dm.WriteDirectory(directory)
	}
	if df != nil {
		df(directory, &dm)
	}
}
