package medorg

import (
	"io/ioutil"
	"log"
	"strconv"
	"strings"
)

type reffer struct {
	fn   string
	size int64
}

func (rf reffer) Key() string {
	return rf.fn + strconv.FormatInt(rf.size, 10)
}

type trackerMap map[string]string

// TreeWalker walks througha  directory tree
type TreeWalker struct {
	buildComplete bool
	tracker       trackerMap
}

// NewTreeWalker creates a tree walker to control the walking of a directory
func NewTreeWalker() *TreeWalker {
	itm := new(TreeWalker)
	return itm
}
func (tw *TreeWalker) initTrackerMap() {
	if tw.tracker == nil {
		tw.tracker = make(map[string]string)
	}
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

			if ok {
				//log.Println("Found File", fn)
				if wf != nil {
					// Annoying syntax to ensure the worker function
					// always gets run
					updateTmp := wf(directory, fn, fs, &dm)
					update = update || updateTmp
				}
			} else if !okNeeded {
				//} else {
				log.Println("Missing File:", fn, okNeeded)
				if wf != nil {
					// Annoying syntax to ensure the worker function
					// always gets run
					updateTmp := wf(directory, fn, fs, &dm)
					update = update || updateTmp
				}

			} else if tw.buildComplete {
				//	log.Fatal("This should not be possible after UpdateDirectory", directory, fn)
				//} else {
				//	log.Fatal("Que???", okNeeded)
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

func (tw TreeWalker) trackWork(directory string, dm *DirectoryMap) {
	fc := func(fn string, fs FileStruct) {
		if !FileExist(directory, fn) {
			//fmt.Println("File dissapeared", fn)
			keyer := reffer{fn, fs.Size}
			tw.tracker[keyer.Key()] = fs.Checksum
		}
	}
	dm.Range(fc)
}
func (tw TreeWalker) autoPopWork(directory, fn string, fs FileStruct, dm *DirectoryMap) bool {
	_, ok := dm.Get(fn)
	//fmt.Println("File:", fn, ok)
	if ok {
		return false
	}
	fsl := FsFromName(directory, fn)
	keyer := reffer{fn, fsl.Size}
	cSum, ok := tw.tracker[keyer.Key()]
	//fmt.Println("Found a file that does not exist", fn, keyer.Key(), ok)
	if ok {
		fsl.Checksum = cSum
		dm.Add(fsl)
		delete(tw.tracker, keyer.Key())

		return true
	}
	return false
}

// MoveDetect
func (tw *TreeWalker) MoveDetect(directories []string) {
	tw.initTrackerMap()
	for _, directory := range directories {
		tw.WalkTree(directory, nil, tw.trackWork)
	}
	for _, directory := range directories {
		tw.WalkTreeMaster(directory, tw.autoPopWork, nil, false)
	}
}
