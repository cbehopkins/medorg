package core

import (
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Debug when true prevents making file changes during debugging
// Currently only used in test bench scenarios
var Debug bool

type DirectoryMapMod func(DirectoryMap, string)

// Md5FileName is the filename we use to save the data in
const Md5FileName = ".medorg.xml"

// ErrSkipCheck Reports a checksum that we have skipped producing
var ErrSkipCheck = errors.New("skipping Checksum")

// NewChannels creates a channel based method for creating checksums
func NewChannels() (inputChan chan FileStruct, outputChan chan FileStruct, closedChan chan struct{}) {
	inputChan = make(chan FileStruct)
	outputChan = make(chan FileStruct)
	closedChan = make(chan struct{})
	go md5Calcer(inputChan, outputChan, closedChan)
	return
}

func md5Calcer(inputChan chan FileStruct, outputChan chan FileStruct, closedChan chan struct{}) {
	for itm := range inputChan {
		// Calculate the MD5 here and send it
		cks, err := CalcMd5File(itm.directory, itm.Name)
		if err != nil {
			log.Printf("Calculation error for %s: %v", itm.Name, err)
			// Skip this file and continue processing
			continue
		}
		log.Println("Calculation for", itm.Name, " complete")
		itm.Checksum = cks
		outputChan <- itm
	}
	log.Println("md5Calcer closed")
	close(outputChan)
	close(closedChan)
}

// newXMLManager creates a new file manager
// This receives FileStructs and stores those contents in
// an appropriate .md5_file.xml
// Note there is now CalcBuffer which will cache open structs
// This trades memory for cpu & IO
// Errors are sent on the returned error channel but do not stop processing of other files
func newXMLManager(inputChan chan FileStruct) (*sync.WaitGroup, chan error) {
	var wg sync.WaitGroup
	errChan := make(chan error, 10) // Buffered to prevent goroutine from blocking
	wg.Add(1)
	go managerWorker(inputChan, &wg, errChan)
	return &wg, errChan
}

func managerWorker(inputChan chan FileStruct, wg *sync.WaitGroup, errChan chan error) {
	defer close(errChan)
	for fs := range inputChan {
		if fs.directory == "" {
			errChan <- fmt.Errorf("empty directory description for file %s, skipping", fs.Name)
			continue
		}
		if err := appendXML(fs.directory, []FileStruct{fs}); err != nil {
			errChan <- fmt.Errorf("error appending XML for file %s in directory %s: %w", fs.Name, fs.directory, err)
			continue
		}
	}
	log.Println("managerWorker closing")
	wg.Done()
}

// appendXML - append items to the existing Xml File
func appendXML(directory string, fsA []FileStruct) error {
	dm, err := DirectoryMapFromDir(directory)
	if err != nil {
		return fmt.Errorf("error loading directory map for %s: %w", directory, err)
	}

	// Add in the items in the input
	for _, fs := range fsA {
		// Check each item to make sure it matches the current directory
		if fs.directory == directory {
			dm.Add(fs)
		} else {
			log.Printf("Warning: directory mismatch, expected %s but got %s, skipping file %s", directory, fs.directory, fs.Name)
			continue
		}
	}
	if err := dm.Persist(directory); err != nil {
		return fmt.Errorf("error persisting directory map for %s: %w", directory, err)
	}
	return nil
}

// ReturnChecksumString gets the hash into the format we like it
// This allows an external tool to calculate the sum
func ReturnChecksumString(h hash.Hash) string {
	return base64.StdEncoding.WithPadding(base64.NoPadding).EncodeToString([]byte(h.Sum(nil)))
}

// CalcMd5File calculates the checksum for a specified filename
func CalcMd5File(directory, fn string) (string, error) {
	fp := filepath.Join(directory, fn)
	f, err := os.Open(fp)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return ReturnChecksumString(h), nil
}

// ProgressCallback is called with byte count as data is being processed
type ProgressCallback func(bytesProcessed int64, timestamp time.Time)

// CalcMd5FileWithProgress calculates the checksum while reporting progress
// callback is called with the number of bytes processed and current timestamp
// This allows real-time throughput monitoring during large file checksums
func CalcMd5FileWithProgress(directory, fn string, callback ProgressCallback) (string, error) {
	fp := filepath.Join(directory, fn)
	f, err := os.Open(fp)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()

	h := md5.New()

	// Create a progress-tracking reader that wraps the file
	pr := &progressReader{
		reader:   f,
		hash:     h,
		callback: callback,
	}

	// Copy with progress tracking
	if _, err := io.Copy(io.Discard, pr); err != nil {
		return "", err
	}

	return ReturnChecksumString(h), nil
}

// progressReader wraps an io.Reader to track bytes and report progress
type progressReader struct {
	reader   io.Reader
	hash     hash.Hash
	callback ProgressCallback
	bytes    int64
}

func (pr *progressReader) Read(p []byte) (n int, err error) {
	n, err = pr.reader.Read(p)
	if n > 0 {
		// Write to hash (if hash is provided)
		if pr.hash != nil {
			if _, hashErr := pr.hash.Write(p[:n]); hashErr != nil {
				return n, hashErr
			}
		}

		// Update byte count and call progress callback with delta
		pr.bytes += int64(n)
		if pr.callback != nil {
			pr.callback(int64(n), time.Now())
		}
	}
	return n, err
}

// Calculator is useful where we get streams of bytes in (e.g. from the network)
// We expose an io.Writer
// close the trigger chanel then wait for the writes to finish
func Calculator(fp string) (iw io.Writer, trigger chan struct{}, wg *sync.WaitGroup) {
	trigger = make(chan struct{})
	wg = new(sync.WaitGroup)
	iw = md5Calc(trigger, wg, fp)
	return
}

func md5CalcInternal(h hash.Hash, wgl *sync.WaitGroup, fpl string, trigger chan struct{}) {
	directory, fn := filepath.Split(fpl)
	dm, err := DirectoryMapFromDir(directory)
	if err != nil {
		log.Printf("Error loading directory map for %s: %v", directory, err)
		wgl.Done()
		return
	}
	completeCalc(trigger, directory, fn, h, dm)
	if err := dm.Persist(directory); err != nil {
		log.Printf("Error persisting directory map for %s: %v", directory, err)
	}
	wgl.Done()
}

func completeCalc(trigger chan struct{}, directory string, fn string, h hash.Hash, dm DirectoryMap) {
	tr := logSlow("CompleteCalc" + fn)
	<-trigger
	defer close(tr)
	fs, err := NewFileStruct(directory, fn)
	if err != nil {
		log.Printf("Error creating FileStruct for %s/%s: %v, skipping", directory, fn, err)
		return
	}
	fs.Checksum = ReturnChecksumString(h)
	dm.Add(fs)
}

// CalcBuffer holds onto writing the directory until later
// Intermittantly writes
type CalcBuffer struct {
	sync.Mutex
	buff   map[string]*DirectoryMap
	closer chan struct{}
	wg     sync.WaitGroup
}

// NewCalcBuffer return a calc buffer
func NewCalcBuffer() *CalcBuffer {
	itm := new(CalcBuffer)
	itm.closer = make(chan struct{})
	itm.buff = make(map[string]*DirectoryMap)
	return itm
}

// Close the calcbuffer and write everything out
func (cb *CalcBuffer) Close() {
	close(cb.closer)
	cb.wg.Add(1)
	cb.wg.Done()
	cb.wg.Wait()
	for dir, dm := range cb.buff {
		if err := dm.Persist(dir); err != nil {
			log.Printf("Error persisting directory map for %s: %v", dir, err)
		}
	}
}

func md5Calc(trigger chan struct{}, wg *sync.WaitGroup, fp string) (iw io.Writer) {
	h := md5.New()
	iw = io.Writer(h)
	wg.Add(1)
	go md5CalcInternal(h, wg, fp, trigger)
	return
}

// Calculate the result for the supplied file path
func (cb *CalcBuffer) Calculate(fp string) (iw io.Writer, trigger chan struct{}) {
	trigger = make(chan struct{})
	h := md5.New()
	iw = io.Writer(h)
	cb.wg.Add(1)
	tr := logSlow("Calculate" + fp)
	go func() {
		cb.calcer(fp, h, trigger)
		close(tr)
		cb.wg.Done()
	}()
	return
}

func (cb *CalcBuffer) calcer(fp string, h hash.Hash, trigger chan struct{}) {
	dm, dir, fn := cb.getFp(fp)
	completeCalc(trigger, dir, fn, h, *dm)
}

func (cb *CalcBuffer) getFp(fp string) (dm *DirectoryMap, dir, fn string) {
	dir, fn = filepath.Split(fp)
	dm = cb.getDir(dir)
	return
}

// getDir returns a (cached) DirectoryMap for the directory in question
func (cb *CalcBuffer) getDir(dir string) (dm *DirectoryMap) {
	var ok bool
	cb.Lock()
	dm, ok = cb.buff[dir]
	cb.Unlock()
	if ok {
		return
	}

	dmL, err := DirectoryMapFromDir(dir)
	if err != nil {
		log.Printf("Error loading directory map for %s: %v", dir, err)
		// Return empty DirectoryMap to avoid nil pointer issues
		dmL = *NewDirectoryMap()
	}
	dm = &dmL
	cb.Lock()
	cb.buff[dir] = dm
	cb.Unlock()
	return
}

func logSlow(fn string) chan struct{} {
	startTime := time.Now()
	closeChan := make(chan struct{})
	go func() {
		if Debug {
			log.Println("Started computing:\"", fn, "\"", " At:", startTime)
			defer log.Println("Finished computing:\"", fn, "\"", " At:", time.Now())
		}
		for {
			select {
			case <-closeChan:
				return
			case <-time.After(time.Minute):
				if Debug {
					log.Println("Still Computing:\"", fn, "\"", " After:", time.Since(startTime))
				}
			}
		}
	}()
	return closeChan
}
