package medorg

import (
	"io/ioutil"
	"log"
	"strings"
)

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

// WalkTree wlak through a directory tree, runnind the specified
// walk func is the func to run on the file walked
// direct func is run every directory and should call itself recursively
func (tw TreeWalker) WalkTree(directory string, wf WalkFunc, df DirectFunc) {
	dm := DirectoryMapFromDir(directory)
	// Now read in all files in the current directory
	stats, err := ioutil.ReadDir(directory)
	if err != nil {
		log.Fatal(err)
	}
	var update bool
	// Put the token back so we will always be able to
	// recurse at least once
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
			tw.WalkTree(nd, wf, df)
			if Debug {
				log.Println("Finished with directory:", nd)
			}
		} else {
			var fs FileStruct
			fs, ok := dm.Get(fn)

			if ok {
				if wf != nil {
					// Annoying syntax to ensure the worker function
					// always gets run
					updateTmp := wf(directory, fn, fs, &dm)
					update = update || updateTmp
				}
			} else if tw.buildComplete {
				log.Fatal("This should not be possible after UpdateDirectory", directory, fn)
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
