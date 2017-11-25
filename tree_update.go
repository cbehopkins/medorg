package medorg

import (
	"log"
	"sync"
)

// TreeUpdate runs through a tree and updates stuff
type TreeUpdate struct {
	walkerToken chan struct{}
	calcToken   chan struct{}
	closeChan   chan struct{}
	pendToken   chan struct{}
	wg          *sync.WaitGroup
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
	tu.walkerToken = make(chan struct{})
	tu.calcToken = make(chan struct{})
	tu.pendToken = make(chan struct{})
	tu.closeChan = make(chan struct{})
	tu.wg = new(sync.WaitGroup)
	tu.wg.Add(3)
	go tu.worker(walkCount, tu.walkerToken)
	go tu.worker(calcCount, tu.calcToken)
	go tu.worker(pendCount, tu.pendToken)
	return
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
	<-tu.walkerToken
	walkFunc(directory, walkFunc)
	tu.walkerToken <- struct{}{}
	tu.Close()
}
