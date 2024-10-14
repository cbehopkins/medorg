package medorg

import (
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)
type barDirHandler struct {
	dirBar               *mpb.Bar
	theMap               map[string]*mpb.Bar
	progressBars         *mpb.Progress
	suppressProgressBars bool
	mapLock 			 *sync.Mutex
}

func NewBarDirHandler(suppressProgressBars bool) *barDirHandler {
	progressBars := mpb.New()
	bh := barDirHandler{}
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
func (bh *barDirHandler) OpenDir(path string) {
	if bh.suppressProgressBars {
		return
	}
	dir_count, err := countImmediateDirectories(path)
	if err != nil {
		panic(err)
	}
	ourBar:= bh.progressBars.AddBar(
		0,
		mpb.PrependDecorators(decor.Name(path)),
		mpb.AppendDecorators(decor.CountersNoUnit("%d / %d")),
	)
	ourBar.SetTotal(int64(dir_count), false)
	parent := filepath.Dir(path)
	
	bh.mapLock.Lock()
	defer bh.mapLock.Unlock()
	if _, ok := bh.theMap[parent]; ok {
		bh.theMap[parent].Increment()
	}
	bh.theMap[path] = ourBar
}
func (bh *barDirHandler) CloseDir(path string) {
	bh.mapLock.Lock()
	defer bh.mapLock.Unlock()
	if bh.suppressProgressBars {
		return
	}
	bh.theMap[path].Abort(true)
	bh.theMap[path].Wait()
	delete(bh.theMap, path)
}
func getFileSize(filePath string) (int64, error) {
    fileInfo, err := os.Stat(filePath)
    if err != nil {
        return 0, err
    }
    return fileInfo.Size(), nil
}
// Define a struct that embeds the ReadCloser
type customReadCloser struct {
    io.ReadCloser
	md5CalcBar *mpb.Bar
	name string
}

// Implement the Read method to delegate to the embedded ReadCloser
func (crc *customReadCloser) Read(p []byte) (n int, err error) {
    i, err :=  crc.ReadCloser.Read(p)
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
func (bh *barDirHandler) FileVisitor(dir_tracker *DirTracker, directory, file string, dm DirectoryMap) func (r io.ReadCloser) io.ReadCloser {
	if bh.suppressProgressBars {
		return nil
	}
	bh.dirBar.SetCurrent(dir_tracker.Value())
	bh.dirBar.SetTotal(dir_tracker.Total(), false)

	bh.mapLock.Lock()
	defer bh.mapLock.Unlock()
	ourDirHandler, ok := bh.theMap[directory]
	if !ok {
		panic("Path not found: " + directory)
	}
	if ourDirHandler == nil {
		panic("DirHandler is nil")
	}

	return func(r io.ReadCloser) io.ReadCloser {
		fileSize, _ := getFileSize(directory + "/" + file)
		md5CalcBar := bh.progressBars.AddBar(fileSize,
			mpb.BarRemoveOnComplete(),
			mpb.PrependDecorators(
				decor.Name("Calculating MD5: " + file),
				decor.CountersKibiByte("% .2f / % .2f"),
			),
			mpb.AppendDecorators(
				decor.Percentage(),
			),)
		return &customReadCloser{r, md5CalcBar, file}
	}
}
