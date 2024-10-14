package medorg

import (
	"path/filepath"
	"strings"
	"sync"
)

// lastPath is a struct that holds the last path we visited
// It is used to determine if we have gone out of scope
type lastPath struct {
	sync.Mutex
	path      string
	callbacks map[string]func(string)
}

func isChildPath(ref, candidate string) (bool, error) {

	rp, err := filepath.Abs(ref)
	if err != nil {
		return false, err
	}
	can, err := filepath.Abs(candidate)
	if err != nil {
		return false, err
	}

	return strings.Contains(rp, can), nil
}

func (p *lastPath) Get() string {
	p.Lock()
	defer p.Unlock()
	return p.path
}
func (p *lastPath) Set(path string) {
	p.Lock()
	defer p.Unlock()
	p.path = path
}
func (p *lastPath) Visit(path string, closerFunc func(string)) error {
	// The job of this is to work out if we have gone out of scope
	// i.e. close /fred/bob if we have received /fred/steve
	// but do not close /fred or /fred/bob when we receive /fred/bob/steve
	// But also, not doing anything is fine!
	p.Lock()
	defer p.Unlock()
	if p.callbacks == nil {
		p.callbacks = make(map[string]func(string))
	}
	defer func() {
		p.path = path
		p.callbacks[path] = closerFunc
	}()

	toCall := make([]string, 0)
	for candPath := range p.callbacks {
		// True for /bob/fred and /bob
		// False for /bob/fred and /bob/steve
		isChild, err := isChildPath(path, candPath)
		if err != nil {
			continue
		}
		if !isChild {
			// We are done with it
			toCall = append(toCall, candPath)
		}
	}
	for _, candPath := range toCall {
		p.callbacks[candPath](candPath)
		delete(p.callbacks, candPath)
	}
	return nil
}
