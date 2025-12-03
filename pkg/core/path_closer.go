package core

import (
	"path/filepath"
	"strings"
	"sync"
)

// lastPath is a struct that holds the last path we visited
// It is used to determine if we have gone out of scope
type lastPath struct {
	sync.Mutex
	path string
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

func (p *lastPath) Closer(path string, closerFunc func(string)) error {
	// The job of this is to work out if we have gone out of scope
	// i.e. close /fred/bob if we have received /fred/steve
	// but do not close /fred or /fred/bob when we receive /fred/bob/steve
	// But also, not doing anything is fine!

	defer p.Set(path)
	prevPath := p.Get()
	if prevPath == "" {
		return nil
	}
	// TODO: make it possible to select this/another/default to this
	shouldClose := func(path string) (bool, error) {
		isChild, err := isChildPath(path, prevPath)
		if err != nil {
			return false, err
		}

		return !isChild, nil
	}
	cl, err := shouldClose(path)
	if err != nil {
		return err
	}
	if cl && closerFunc != nil {
		closerFunc(prevPath)
	}
	return nil
}
