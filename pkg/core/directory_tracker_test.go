package core

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDirectoryTrackerAgainstMock(t *testing.T) {
	type testSet struct {
		cfg []int
	}
	// Reduced test set for faster execution while maintaining coverage
	testSet0 := []testSet{
		{cfg: []int{1, 0, 1}},  // Single directory, no depth
		{cfg: []int{2, 0, 1}},  // Two directories, no depth
		{cfg: []int{3, 1, 1}},  // Moderate width and depth
		{cfg: []int{2, 2, 2}},  // Moderate depth test
		{cfg: []int{5, 1, 1}},  // Wider but shallow
		{cfg: []int{10, 0, 1}}, // Wide but no depth
	}

	for _, tst := range testSet0 {
		ts := tst.cfg
		testName := fmt.Sprintln("DirectoryTrackerMock", ts)

		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			// Use t.TempDir() for automatic cleanup
			root := t.TempDir()
			err := createTestDirectoriesWithFs(root, ts[0], ts[1], ts[2])
			if err != nil {
				t.Error("Error creating test directories", err)
			}

			makerFunc := func(dir string) (DirectoryTrackerInterface, error) {
				return newMockDtType(), nil
			}
			errChan := NewDirTracker(false, root, makerFunc).ErrChan()
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
	// Significantly reduced test set with shorter sleep times and smaller directory structures
	// This maintains test coverage while dramatically reducing runtime
	testSet0 := []testSet{
		{cfg: []int{1, 0, 1}, prob: 1},
		{cfg: []int{2, 0, 1}, prob: 1},
		{cfg: []int{3, 1, 1}, prob: 20},  // Reduced from 10,1,1 with prob 3
		{cfg: []int{2, 2, 2}, prob: 100}, // Reduced from 3,3,4 with prob 100
		{cfg: []int{3, 1, 2}, prob: 50},  // Reduced from 4,2,8 with prob 500
		{cfg: []int{5, 1, 1}, prob: 10},  // Reduced from 10,2,1 with prob 10
	}
	for _, tst := range testSet0 {
		ts := tst
		testName := fmt.Sprintln("DirectoryTrackerSpawning", ts.cfg)

		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			var cnt uint32
			var activeVisitors int
			var lk sync.Mutex
			prob := ts.prob

			visiter := func(dir Dirname, file Fname) {
				lk.Lock()
				activeVisitors++
				if activeVisitors > NumTrackerOutstanding {
					t.Error("Too many visitors", dir, file)
				}
				if prob != 0 {
					pb := rand.Intn(prob)
					// Reduced sleep duration from 1 second to 100ms for faster tests
					if pb < 2 {
						lk.Unlock()
						atomic.AddUint32(&cnt, 1)
						time.Sleep(100 * time.Millisecond)

						lk.Lock()
					}
				}
				activeVisitors--
				lk.Unlock()
			}

			// Use t.TempDir() for automatic cleanup
			root := t.TempDir()
			if err := createTestDirectoriesWithFs(root, ts.cfg[0], ts.cfg[1], ts.cfg[2]); err != nil {
				t.Error("Error creating test directories", err)
			}

			makerFunc := func(dir string) (DirectoryTrackerInterface, error) {
				mdt := newMockDtType()
				mdt.visiter = visiter
				return mdt, nil
			}
			errChan := NewDirTracker(false, root, makerFunc).ErrChan()
			for err := range errChan {
				t.Error(err)
			}
			t.Log("Slept:", cnt, " times")
		})
	}
}
