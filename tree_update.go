package medorg

import (
	"log"
	"sync"
)

// TreeUpdate runs through a tree and updates stuff
type TreeUpdate struct {
	walkCount, calcCount, pendCount int
	walkerToken                     chan struct{}
	calcToken                       chan struct{}
	closeChan                       chan struct{}
	pendToken                       chan struct{}
	wg                              *sync.WaitGroup

	tm *trackerMap
}

// Our Worker will allow up to items to be issued as tokens
func (tu TreeUpdate) worker(items int, ch chan struct{}) {
	var outstandingTokens int
	var writeChan chan struct{}
	if items > 0 {
		writeChan = ch
	}
	var closed bool

	for !closed || (outstandingTokens > 0) {
		select {
		case <-tu.closeChan:
			closed = true
			writeChan = nil
		case writeChan <- struct{}{}:
			outstandingTokens++
			if outstandingTokens >= items {
				writeChan = nil
			}
		case <-ch:
			if outstandingTokens <= 0 {
				log.Fatal("Negative tokens")
			}
			outstandingTokens--
			if !closed {
				writeChan = ch
			}
		}
	}
	tu.wg.Done()
}

// NewTreeUpdate creates a tree walker
// walkCount = how many directory walkers
// calcCount - How many calculate engines
// pendCount - How many things can actually be accessing the disk at once
func NewTreeUpdate(walkCount, calcCount, pendCount int) (tu TreeUpdate) {
	tu.walkCount, tu.calcCount, tu.pendCount = walkCount, calcCount, pendCount
	tu.Init()
	return
}

// Init initialise the struct
func (tu *TreeUpdate) Init() {
	tu.walkerToken = make(chan struct{})
	tu.calcToken = make(chan struct{})
	tu.pendToken = make(chan struct{})
	tu.closeChan = make(chan struct{})
	tu.wg = new(sync.WaitGroup)
	tu.wg.Add(3)
	go tu.worker(tu.walkCount, tu.walkerToken)
	go tu.worker(tu.calcCount, tu.calcToken)
	go tu.worker(tu.pendCount, tu.pendToken)
}

// ModifyFunc is what is called duriong the walk to allow modification of the fs
type ModifyFunc func(dir, fn string, fs FileStruct) (FileStruct, bool)

// CalcingFunc is a function that is called to calculate the result wanted
type CalcingFunc func(dir, fn string) (string, error)

// WalkingFunc A walking funciton is one that walks the tree - it will probably recurse
type WalkingFunc func(dir string, wkf WalkingFunc)

// Close is a starnadrd close function
func (tu TreeUpdate) Close() {
	close(tu.closeChan)
	tu.wg.Wait()
}

// UpdateDirectory Commands the update of a tree
func (tu TreeUpdate) UpdateDirectory(directory string, mf ModifyFunc) {

	tmpFunc := func(dir, fn string) (string, error) {
		if Debug {
			log.Println("Attempting to get cal token for:", dir, "/", fn)
		}
		<-tu.calcToken
		defer func() {
			tu.calcToken <- struct{}{}
		}()
		return CalcMd5File(dir, fn)
	}

	walkFunc := func(dir string, wkf WalkingFunc) {
		if Debug {
			log.Println("Attemping to get walk token for:", dir)
		}
		updateDirectory(dir, tmpFunc, wkf, tu.pendToken, tu.walkerToken, mf)
	}

	tu.WalkDirectory(directory, walkFunc)

}

// WalkDirectory walk the supplied directory using the walkfunc supplied
func (tu TreeUpdate) WalkDirectory(directory string, walkFunc WalkingFunc) {
	<-tu.walkerToken
	walkFunc(directory, walkFunc)
	tu.walkerToken <- struct{}{}
	tu.Close()
}

func (tu *TreeUpdate) getChecksum(keyer reffer) (string, bool) {
	tu.tm.lk.RLock()
	defer tu.tm.lk.RUnlock()
	cSum, ok := tu.tm.tm[keyer.Key()]
	return cSum, ok
}
func (tu *TreeUpdate) retrieveChecksum(dir, fn string) (string, error) {
	fsl, err := NewFileStruct(dir, fn)
	if err != nil {
		return "", ErrSkipCheck
	}
	// Here's the clever bit!
	// If we find a file of the same name and size with a checksum entry
	// i.e. a file that looked like it was deleted
	// then it's the file, but moved.
	cSum, ok := tu.getChecksum(reffer{fn, fsl.Size})

	if ok {
		return cSum, nil
	}
	return "", ErrSkipCheck
}

// Look for files that are missing and save their size & checksums
func (tu *TreeUpdate) collectMissingFileChecksums(dir string, wkf WalkingFunc) {
	dm := DirectoryMapFromDir(dir)
	tu.tm.trackWork(dir, &dm)
	walkDirectory(dir, nil, wkf, tu.pendToken, tu.walkerToken, nil, dm)
}

// MoveDetect detect moved files by looking for files of same name and size
// that have gone missing elsewhere
func (tu *TreeUpdate) MoveDetect(directories []string) {
	tu.tm = newTrackerMap()

	<-tu.walkerToken
	for _, directory := range directories {
		tu.collectMissingFileChecksums(directory, tu.collectMissingFileChecksums)
	}
	tu.walkerToken <- struct{}{}
	tu.Close()

	tu.Init()
	<-tu.walkerToken
	pf := func(dir string, wkf WalkingFunc) {
		dm := DirectoryMapFromDir(dir)
		walkDirectory(dir, tu.retrieveChecksum, wkf, tu.pendToken, tu.walkerToken, nil, dm)
	}
	for _, directory := range directories {
		pf(directory, pf)
	}
	tu.walkerToken <- struct{}{}
	tu.Close()
}
