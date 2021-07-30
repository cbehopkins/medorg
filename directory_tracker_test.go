package medorg

import (
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockDtType struct {
	errChan chan error
	lock    *sync.RWMutex
	closed  *bool
	visiter func(string, string)
}

func newMockDtType() (mdt mockDtType) {
	mdt.errChan = make(chan error)
	mdt.lock = new(sync.RWMutex)
	mdt.closed = new(bool)
	return
}

func (mdt mockDtType) ErrChan() <-chan error {
	return mdt.errChan
}
func (mdt mockDtType) Start() error {
	return nil
}
func (mdt mockDtType) Close() {
	mdt.lock.Lock()
	*mdt.closed = true
	mdt.lock.Unlock()
	close(mdt.errChan)
}

var errTestChanClosed = errors.New("visit called to a closed structure")

func (mdt mockDtType) VisitFile(dir, file string, d fs.DirEntry, callback func()) {

	mdt.lock.Lock()
	if *mdt.closed {
		mdt.errChan <- fmt.Errorf("%w at %s/%s", errTestChanClosed, dir, file)
	}
	mdt.lock.Unlock()

	if mdt.visiter != nil {
		mdt.visiter(dir, file)
	}
	callback()
}

func TestDirectoryTrackerAgainstMock(t *testing.T) {

	type testSet struct {
		cfg []int
	}
	testSet0 := []testSet{
		{cfg: []int{1, 0, 1}},
		{cfg: []int{1, 1, 1}},
		{cfg: []int{2, 0, 1}},
		{cfg: []int{10, 1, 1}},
		{cfg: []int{3, 3, 4}},
		{cfg: []int{4, 2, 8}},
		// {cfg: []int{6, 4, 2}},
		{cfg: []int{10, 2, 1}},
		{cfg: []int{100, 0, 1}},
		{cfg: []int{100, 1, 1}},
		{cfg: []int{1000, 0, 1}},
		{cfg: []int{10000, 0, 1}},
	}

	for _, tst := range testSet0 {
		ts := tst.cfg
		testName := fmt.Sprintln("DirectoryTrackerMock", ts)

		t.Run(testName, func(t *testing.T) {
			root, err := createTestMoveDetectDirectories(ts[0], ts[1], ts[2])
			if err != nil {
				t.Error("Error creating test directories", err)
			}
			defer os.RemoveAll(root)

			makerFunc := func(dir string) (DirectoryTrackerInterface, error) {
				return newMockDtType(), nil
			}
			errChan := NewDirTracker(root, makerFunc).ErrChan()
			for err := range errChan {
				t.Error(err)
			}
		})
	}
}

// FIXME we need a further variant on this test that
// e.g. on close directory does a file write to mimic
// the real system
// As if we're not careful we can get a resource spam here
// as everything closes at once
func TestDirectoryTrackerSpawning(t *testing.T) {

	type testSet struct {
		cfg  []int
		prob int
	}
	testSet0 := []testSet{
		// {cfg: []int{1, 0, 1}, prob: 1},
		// {cfg: []int{1, 1, 1}, prob: 1},
		// {cfg: []int{2, 0, 1}, prob: 1},
		// {cfg: []int{10, 1, 1}, prob: 3},
		// {cfg: []int{3, 3, 4}, prob: 100},
		// {cfg: []int{4, 2, 8}, prob: 500},
		// {cfg: []int{10, 2, 1}, prob: 10},
		{cfg: []int{100, 0, 1}, prob: 1},
		{cfg: []int{100, 1, 1}, prob: 200},
		{cfg: []int{1000, 0, 1}, prob: 250},
		{cfg: []int{10000, 0, 1}, prob: 400},
	}
	var activeVisitors int
	var lk sync.Mutex

	for _, tst := range testSet0 {
		var cnt uint32
		ts := tst.cfg
		testName := fmt.Sprintln("DirectoryTrackerMock", ts)
		visiter := func(dir, file string) {
			lk.Lock()
			activeVisitors++
			if activeVisitors > NumTrackerOutstanding {
				t.Error("Too many visitors", dir, file)
			}
			if tst.prob != 0 {
				pb := rand.Intn(tst.prob)
				if pb < 2 {
					lk.Unlock()
					atomic.AddUint32(&cnt, 1)
					time.Sleep(time.Second)

					lk.Lock()
				}
			}
			activeVisitors--
			lk.Unlock()
		}

		t.Run(testName, func(t *testing.T) {
			root, err := createTestMoveDetectDirectories(ts[0], ts[1], ts[2])
			if err != nil {
				t.Error("Error creating test directories", err)
			}
			defer os.RemoveAll(root)

			makerFunc := func(dir string) (DirectoryTrackerInterface, error) {
				mdt := newMockDtType()
				mdt.visiter = visiter
				return mdt, nil
			}
			errChan := NewDirTracker(root, makerFunc).ErrChan()
			for err := range errChan {
				t.Error(err)
			}
		})
		t.Log("Slept:", cnt, " times")
	}
}
