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
		lp.Visit(path, myCloser)

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
	checkRun("/bob/fred/w2", 3)
	checkRun("/bob/fred/w2/w4", 3)
	// Both w2 and w4 should now be closed
	checkRun("/bob/fred/w3", 5)
	checkRun("/bob/fred", 6)

}
