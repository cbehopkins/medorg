package core

// MakeTokenChan creates a buffered channel with a fixed number of tokens for concurrency control
func MakeTokenChan(numOutstanding int) chan struct{} {
	tkc := make(chan struct{}, numOutstanding)
	for range numOutstanding {
		tkc <- struct{}{}
	}
	return tkc
}

const NumTrackerOutstanding = 4
