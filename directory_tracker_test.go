package medorg

import "testing"

func TestPathCloser(t *testing.T) {
	var dt DirTracker
	dt.lastPath = "/bob"
	callCount := 0
	myCloser := func(path string) {
		callCount++
	}
	checkRun := func(path string, cnt int) {
		dt.pathCloser(path, myCloser)

		if callCount != cnt {
			t.Error("Failed on,", path, cnt, callCount, dt.lastPath)
		}
	}

	checkRun("/bob/fred", 0)
	checkRun("/bob/fred/bob", 0)
	checkRun("/bob/fred", 1)
	checkRun("/bob/fred/steve", 1)
	checkRun("/bob/fred/susan", 2)
	checkRun("/bob/fred", 3)

}
