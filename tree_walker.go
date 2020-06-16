package medorg

import (
	"io/ioutil"
	"log"
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
	tm map[string]string
	lk *sync.RWMutex
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

//newTrackerMap create a new tracker of file structs
func newTrackerMap() *trackerMap {
	tm := new(trackerMap)
	tm.tm = make(map[string]string)
	tm.lk = new(sync.RWMutex)
	return tm
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

// WalkTree wlak through a directory tree, runnind the specified
// walk func is the func to run on the file walked
// direct func is run every directory and should call itself recursively
func (tw TreeWalker) WalkTree(directory string, wf WalkFunc, df DirectFunc) {
	tw.WalkTreeMaster(directory, wf, df, true)
}

// WalkTreeMaster as WalkTree but allows more programmability
func (tw TreeWalker) WalkTreeMaster(directory string, wf WalkFunc, df DirectFunc, okNeeded bool) {
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
			nd := directory + "/" + fn
			log.Println("Going into walk directory:", nd)
			tw.WalkTreeMaster(nd, wf, df, okNeeded)
			if Debug {
				log.Println("Finished with directory:", nd)
			}
		} else {
			var fs FileStruct
			fs, ok := dm.Get(fn)

			if ok || !okNeeded {
				//log.Println("Found File", fn)
				if wf != nil {
					// Annoying syntax to ensure the worker function
					// always gets run
					updateTmp := wf(directory, fn, fs, &dm)
					update = update || updateTmp
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

func (tw trackerMap) trackWork(directory string, dm *DirectoryMap) {
	fc := func(fn string, fs FileStruct) {
		if !FileExist(directory, fn) {
			keyer := reffer{fn, fs.Size}
			tw.lk.Lock()
			tw.tm[keyer.Key()] = fs.Checksum
			tw.lk.Unlock()
		}
	}
	dm.Range(fc)
}
