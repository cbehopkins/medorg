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

	tu.WalkDirectory(directory, walkFunc, mf)

}

// WalkDirectory walk the supplied directory using the walkfunc supplied
// TBD remove ModifyFunc as unused
func (tu TreeUpdate) WalkDirectory(directory string, walkFunc WalkingFunc, mf ModifyFunc) {
	<-tu.walkerToken
	walkFunc(directory, walkFunc)
	tu.walkerToken <- struct{}{}
	tu.Close()
}

// MoveDetect detect moved files by looking for files of same name and size
// that have gone missing elsewhere
func (tu *TreeUpdate) MoveDetect(directories []string) {
	tu.tm = newTrackerMap()
	cf := func(dir, fn string) (string, error) {
		fsl := FsFromName(dir, fn)
		keyer := reffer{fn, fsl.Size}
		tu.tm.lk.RLock()
		cSum, ok := tu.tm.tm[keyer.Key()]
		tu.tm.lk.RUnlock()

		if ok {
			return cSum, nil
		}
		return "", ErrSkipCheck
	}
	wf := func(dir string, wkf WalkingFunc) {
		dm := DirectoryMapFromDir(dir)
		tu.tm.trackWork(dir, &dm)
		//log.Println("Into:", dir)
		walkDirectory(dir, nil, wkf, tu.pendToken, tu.walkerToken, nil, dm)
		//log.Println("Out of:", dir)
	}

	<-tu.walkerToken
	for _, directory := range directories {
		//.WalkTree(directory, nil, tm.trackWork)
		wf(directory, wf)
		//log.Println("Walked:", directory)
	}
	tu.walkerToken <- struct{}{}
	tu.Close()

	tu.Init()
	<-tu.walkerToken
	pf := func(dir string, wkf WalkingFunc) {
		dm := DirectoryMapFromDir(dir)
		//log.Println("Pop Into:", dir)
		walkDirectory(dir, cf, wkf, tu.pendToken, tu.walkerToken, nil, dm)
		//log.Println("Pop Out of:", dir)

	}
	for _, directory := range directories {
		//tu.WalkTreeMaster(directory, tm.autoPopWork, nil, false)
		pf(directory, pf)
	}
	tu.walkerToken <- struct{}{}
	tu.Close()
	tu.Init()
}
