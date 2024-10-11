package medorg

import "testing"

func TestPathCloser(t *testing.T) {
	var lp lastPath
	lp.Set("/bob")
	callCount := 0
	myCloser := func(path string) {
		callCount++
	}
	checkRun := func(path string, cnt int) {
		lp.Closer(path, myCloser)

		if callCount != cnt {
			t.Error("Failed on,", path, cnt, callCount, lp.Get())
		}
	}

	checkRun("/bob/fred", 0)
	checkRun("/bob/fred/bob", 0)
	checkRun("/bob/fred", 1)
	checkRun("/bob/fred/steve", 1)
	checkRun("/bob/fred/susan", 2)
	checkRun("/bob/fred", 3)
}
