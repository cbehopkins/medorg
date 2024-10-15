package medorg

import (
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

// ReadCloserWrap is a function that wraps a ReadCloser
// Useful for adding a progress bar to the calculation functions
type ReadCloserWrap func(r io.ReadCloser, fileSize int64) io.ReadCloser

// BarDirHandler handles progress bars for directories
type BarDirHandler struct {
	dirBar               *mpb.Bar
	theMap               map[string]*mpb.Bar
	progressBars         *mpb.Progress
	suppressProgressBars bool
	mapLock              *sync.Mutex
}

// NewBarDirHandler creates a new barDirHandler
// i.e. a progress bar for directories
func NewBarDirHandler(suppressProgressBars bool) *BarDirHandler {
	progressBars := mpb.New()
	bh := BarDirHandler{}
	bh.suppressProgressBars = suppressProgressBars
	bh.progressBars = progressBars
	bh.dirBar = bh.progressBars.AddBar(
		0,
		mpb.PrependDecorators(decor.Name("Dirs")),
		mpb.AppendDecorators(decor.CountersNoUnit("%d / %d")),
	)
	bh.theMap = make(map[string]*mpb.Bar)
	bh.mapLock = &sync.Mutex{}
	return &bh
}

// OpenDir opens a directory
func (bh *BarDirHandler) OpenDir(path string) {
	if bh.suppressProgressBars {
		return
	}
	dirCount, err := countImmediateDirectories(path)
	if err != nil {
		panic(err)
	}
	ourBar := bh.progressBars.AddBar(
		0,
		mpb.PrependDecorators(decor.Name(path)),
		mpb.AppendDecorators(decor.CountersNoUnit("%d / %d")),
	)
	ourBar.SetTotal(int64(dirCount), false)
	parent := filepath.Dir(path)

	bh.mapLock.Lock()
	defer bh.mapLock.Unlock()
	if _, ok := bh.theMap[parent]; ok {
		bh.theMap[parent].Increment()
	}
	bh.theMap[path] = ourBar
}

// CloseDir closes a directory
func (bh *BarDirHandler) CloseDir(path string) {
	if bh.suppressProgressBars {
		return
	}
	bh.mapLock.Lock()
	defer bh.mapLock.Unlock()
	bh.theMap[path].Abort(true)
	bh.theMap[path].Wait()
	delete(bh.theMap, path)
}

// Define a struct that embeds the ReadCloser
type customReadCloser struct {
	io.ReadCloser
	md5CalcBar *mpb.Bar
	name       string
}

// Implement the Read method to delegate to the embedded ReadCloser
func (crc *customReadCloser) Read(p []byte) (n int, err error) {
	i, err := crc.ReadCloser.Read(p)
	crc.md5CalcBar.IncrBy(i)
	return i, err
}

// Implement the Close method to perform additional actions
func (crc *customReadCloser) Close() error {
	defer func() {
		crc.md5CalcBar.Abort(true)
		crc.md5CalcBar.Wait()
	}()
	return crc.ReadCloser.Close()
}

func countImmediateDirectories(path string) (int, error) {
	var count int
	entries, err := os.ReadDir(path)
	if err != nil {
		return 0, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			count++
		}
	}
	return count, nil
}

// FileVisitor is the function that should be called for each file visited
// it will update the main directory bar and
// create callback to use when we need a progress bar for the file
// The bar is not created immediatly as not all files require significant work
func (bh *BarDirHandler) FileVisitor(dirTracker *DirTracker, directory, file string, dm DirectoryMap) ReadCloserWrap {
	if bh.suppressProgressBars {
		return nil
	}
	bh.dirBar.SetCurrent(dirTracker.Value())
	bh.dirBar.SetTotal(dirTracker.Total(), false)

	return func(r io.ReadCloser, fileSize int64) io.ReadCloser {
		md5CalcBar := bh.progressBars.AddBar(fileSize,
			mpb.BarRemoveOnComplete(),
			mpb.PrependDecorators(
				decor.Name("Calculating MD5: "+file),
				decor.CountersKibiByte("% .2f / % .2f"),
			),
			mpb.AppendDecorators(
				decor.Percentage(),
			))
		return &customReadCloser{r, md5CalcBar, file}
	}
}
