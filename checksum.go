package medorg

import (
	"crypto/md5"
	"encoding/base64"
	"errors"
	"hash"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// Debug if true if we're debugging
// prevents making file changes
// Only used in TB at the moment
var Debug bool

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
		cks, err := CalcMd5File(itm.directory, itm.Name, nil)
		if err != nil {
			log.Fatal("Calculation error", err)
		} else {
			log.Println("Calculation for", itm.Name, " complete")
		}
		itm.Checksum = cks
		outputChan <- itm
	}
	log.Println("md5Calcer closed")
	close(outputChan)
	close(closedChan)
}

// newXMLManager creates a new file manager
// This receives FileStructs and stroes those contents in
// an appropriate .md5_file.xml
// Note there is now CalcBuffer which will cache open structs
// This trades memory for cpu & IO
func newXMLManager(inputChan chan FileStruct) *sync.WaitGroup {
	// FIXME The error management in this is laughable
	// Not a trivial job to fix though
	var wg sync.WaitGroup
	wg.Add(1)
	go managerWorker(inputChan, &wg)
	return &wg
}

func managerWorker(inputChan chan FileStruct, wg *sync.WaitGroup) {
	for fs := range inputChan {
		if fs.directory == "" {
			log.Fatal("Empty Directory description")
		}
		appendXML(fs.directory, []FileStruct{fs})
	}
	log.Println("managerWorker closing")
	wg.Done()
}

// appendXML - append items to the existing Xml File
func appendXML(directory string, fsA []FileStruct) {
	// FIXME error prop
	dm, _ := DirectoryMapFromDir(directory, nil)

	// Add in the items in the input
	for _, fs := range fsA {
		// Check each item to make sure it matches the current directory
		if fs.directory == directory {
			dm.Add(fs)
		} else {
			log.Fatal("directories are incorrect", fs.directory, directory)
		}
	}
	// FIXME
	_ = dm.Persist(directory)
}

// ReturnChecksumString gets the hash into the format we like it
// This allows an external tool to calculate the sum
func ReturnChecksumString(h hash.Hash) string {
	return base64.StdEncoding.WithPadding(base64.NoPadding).EncodeToString([]byte(h.Sum(nil)))
}
func getFileSize(filePath string) (int64, error) {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return 0, err
	}
	return fileInfo.Size(), nil
}

// CalcMd5File calculates the checksum for a specified filename
func CalcMd5File(directory, fn string, readCloserWrap ReadCloserWrap) (string, error) {
	fp := filepath.Join(directory, fn)
	f, err := os.Open(fp)
	if err != nil {
		return "", err
	}
	var fh io.ReadCloser
	if readCloserWrap != nil {
		fileSize, err := getFileSize(directory + "/" + fn)
		if err != nil {
			return "", err
		}
		fh = readCloserWrap(f, fileSize)
	} else {
		fh = f
	}
	defer func() { _ = fh.Close() }()
	h := md5.New()
	if _, err := io.Copy(h, fh); err != nil {
		return "", err
	}
	return ReturnChecksumString(h), nil
}

// Calculator is useful where we get streams of bytes in (e.g. from the network)
// We expose an io.Writer
// close the trigger chanel then wait for the writes to finish
// func Calculator(fp string) (iw io.Writer, trigger chan struct{}, wg *sync.WaitGroup) {
// 	trigger = make(chan struct{})
// 	wg = new(sync.WaitGroup)
// 	iw = md5Calc(trigger, wg, fp)
// 	return
// }
// func md5CalcInternal(h hash.Hash, wgl *sync.WaitGroup, fpl string, trigger chan struct{}) {
// 	directory, fn := filepath.Split(fpl)
// 	//FIXME error prop
// 	dm, _ := DirectoryMapFromDir(directory)
// 	completeCalc(trigger, directory, fn, h, dm)
// 	// FIXME
// 	_ = dm.Persist(directory)
// 	wgl.Done()
// }
// func completeCalc(trigger chan struct{}, directory string, fn string, h hash.Hash, dm DirectoryMap) {
// 	tr := logSlow("CompleteCalc" + fn)
// 	<-trigger
// 	defer close(tr)
// 	fs, err := NewFileStruct(directory, fn)
// 	if err != nil {
// 		log.Fatal("Error in filename to calc", err)
// 	}
// 	fs.Checksum = ReturnChecksumString(h)
// 	dm.Add(fs)
// }

// CalcBuffer holds onto writing the directory until later
// Intermittantly writes
// type CalcBuffer struct {
// 	sync.Mutex
// 	buff   map[string]*DirectoryMap
// 	closer chan struct{}
// 	wg     sync.WaitGroup
// }

// // NewCalcBuffer return a calc buffer
// func NewCalcBuffer() *CalcBuffer {
// 	itm := new(CalcBuffer)
// 	itm.closer = make(chan struct{})
// 	itm.buff = make(map[string]*DirectoryMap)
// 	return itm
// }

// // Close the calcbuffer and write everything out
// func (cb *CalcBuffer) Close() {
// 	close(cb.closer)
// 	cb.wg.Add(1)
// 	cb.wg.Done()
// 	cb.wg.Wait()
// 	for dir, dm := range cb.buff {
// 		// FIXME
// 		_ = dm.Persist(dir)
// 	}
// }
// func md5Calc(trigger chan struct{}, wg *sync.WaitGroup, fp string) (iw io.Writer) {
// 	h := md5.New()
// 	iw = io.Writer(h)
// 	wg.Add(1)
// 	go md5CalcInternal(h, wg, fp, trigger)
// 	return
// }

// Calculate the result for the supplied file path
// func (cb *CalcBuffer) Calculate(fp string) (iw io.Writer, trigger chan struct{}) {
// 	trigger = make(chan struct{})
// 	h := md5.New()
// 	iw = io.Writer(h)
// 	cb.wg.Add(1)
// 	tr := logSlow("Calculate" + fp)
// 	go func() {
// 		cb.calcer(fp, h, trigger)
// 		close(tr)
// 		cb.wg.Done()
// 	}()
// 	return
// }
// func (cb *CalcBuffer) calcer(fp string, h hash.Hash, trigger chan struct{}) {
// 	dm, dir, fn := cb.getFp(fp)
// 	completeCalc(trigger, dir, fn, h, *dm)
// }

// func (cb *CalcBuffer) getFp(fp string) (dm *DirectoryMap, dir, fn string) {
// 	dir, fn = filepath.Split(fp)
// 	dm = cb.getDir(dir)
// 	return
// }

// getDir returns a (cached) DirectoryMap for the directory in question
// func (cb *CalcBuffer) getDir(dir string) (dm *DirectoryMap) {
// 	var ok bool
// 	cb.Lock()
// 	dm, ok = cb.buff[dir]
// 	cb.Unlock()
// 	if ok {
// 		return
// 	}

// 	// FIXME error prop
// 	dmL, _ := DirectoryMapFromDir(dir)
// 	dm = &dmL
// 	cb.Lock()
// 	cb.buff[dir] = dm
// 	cb.Unlock()
// 	return
// }

// func logSlow(fn string) chan struct{} {
// 	startTime := time.Now()
// 	closeChan := make(chan struct{})
// 	go func() {
// 		if Debug {
// 			log.Println("Started computing:\"", fn, "\"", " At:", startTime)
// 			defer log.Println("Finished computing:\"", fn, "\"", " At:", time.Now())
// 		}
// 		for {
// 			select {
// 			case <-closeChan:
// 				return
// 			case <-time.After(time.Minute):
// 				if Debug {
// 					log.Println("Still Computing:\"", fn, "\"", " After:", time.Since(startTime))
// 				}
// 			}
// 		}
// 	}()
// 	return closeChan
// }
