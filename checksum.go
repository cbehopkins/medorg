package medorg

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Debug if true if we're debugging
// prevents making file changes
// Only used in TB at the moment
var Debug bool

// Md5FileName is the filename we use to save the data in
const Md5FileName = ".md5_list.xml"
const idleWriteDuration = 30 * time.Second

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
		fn := itm.Name
		log.Println("Received fn")
		cks, err := CalcMd5File(".", fn)
		if err != nil {
			log.Fatal("Calculation error", err)
		} else {
			log.Println("Calculation for", fn, " complete")
		}
		itm.Checksum = cks
		outputChan <- itm
	}
	log.Println("md5Calcer closed")
	close(outputChan)
	close(closedChan)
}

// NewXMLManager creates a new file manager
// This receives FileStructs and stroes those contents in
// an appropriate .md5_file.xml
func NewXMLManager(inputChan chan FileStruct) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)
	go managerWorker(inputChan, &wg)
	return &wg
}

func managerWorker(inputChan chan FileStruct, wg *sync.WaitGroup) {
	for fs := range inputChan {
		direct := fs.directory
		if direct == "" {
			log.Fatal("Empty Directory description")
		}
		// TBD create a local cache and flush mechanism here
		appendXML(direct, []FileStruct{fs})
	}
	log.Println("managerWorker closing")
	wg.Done()
}

// appendXML - append items to the existing Xml File
func appendXML(directory string, fsA []FileStruct) {
	dm := DirectoryMapFromDir(directory)
	// Add in the items in the input
	for _, fs := range fsA {
		// Check each item to make sure it matches the current directory
		if fs.directory == directory {
			dm.Add(fs)
		} else {
			log.Fatal("directories are incorrect", fs.directory, directory)
		}
	}
	dm.WriteDirectory(directory)
}

// UpdateDirectory will for a specified directory
// go through and update the xml file
func UpdateDirectory(directory string, mf ModifyFunc) {
	dirCnt := 4
	pendCnt := 4
	dirToken := make(chan struct{}, dirCnt)
	for i := 0; i < dirCnt; i++ {
		dirToken <- struct{}{}
	}
	pendToken := make(chan struct{}, pendCnt)
	for i := 0; i < pendCnt; i++ {
		pendToken <- struct{}{}
	}

	tmpFunc := func(directory, fn string) (string, error) {
		return CalcMd5File(directory, fn)
	}

	walkFunc := func(dir string, wkf WalkingFunc) {
		<-dirToken
		updateDirectory(dir, tmpFunc, wkf, pendToken, dirToken, mf)
		dirToken <- struct{}{}
	}

	walkFunc(directory, walkFunc)
}

func updateDirectory(directory string, calcFunc CalcingFunc, walkFunc WalkingFunc, pendTok, dirTok chan struct{}, mf ModifyFunc) {
	var dwg sync.WaitGroup
	var fwg sync.WaitGroup
	closeChan := make(chan struct{})
	//<-dirTok
	// Reduce the xml to only the items that exist
	dm := reduceXMLFe(directory)

	// Now read in all files in the current directory
	stats, err := ioutil.ReadDir(directory)

	if err != nil {
		log.Fatal(err)
	}
	dirTok <- struct{}{}

	// Spawn the writer that will update the xml
	// Needed in case we abort part way through
	// we don't want to lose the progress we have made
	writerWg := dm.idleWriter(closeChan, directory)
	// Put the token back so we will always be able to
	// recurse at least once
	for _, file := range stats {
		fn := file.Name()
		if strings.HasPrefix(fn, ".") {
			// Don't build for hidden files
			continue
			// If it is a directory, then go into it
		}
		if file.IsDir() {
			nd := directory + "/" + fn
			log.Println("Going into Update directory:", nd)
			dwg.Add(1)
			<-dirTok
			go func(ld string) {
				walkFunc(ld, walkFunc)
				dwg.Done()
				if Debug {
					log.Println("Finished with directory:", ld)
				}
				dirTok <- struct{}{}
			}(nd)
		} else {
			var fs FileStruct
			fs, ok := dm.Get(fn)

			if ok {
				if mf != nil {
					fs, update := mf(directory, fn, fs)
					if update {
						dm.Add(fs)
					}
				}
			} else {
				<-pendTok
				fwg.Add(1)
				go func() {
					fs = FsFromName(directory, fn)
					cs, err := calcFunc(directory, fn)
					pendTok <- struct{}{}
					if err == nil {
						fs.Checksum = cs
						if mf != nil {
							fsLocal, update := mf(directory, fn, fs)
							if update {
								fs = fsLocal
							}
						}
						dm.Add(fs)
					} else {
						log.Fatal("Error back from checksum calculation", err)
					}
					fwg.Done()
				}()
			}
		}
	}
	// we've done with using IO ourselves
	// so allow someone else to
	dwg.Wait()
	fwg.Wait()
	// Now the md struct is up to date
	// write it out
	close(closeChan)
	writerWg.Wait()
	// retrieve the token we're expected to have
	<-dirTok
}

// ReturnChecksumString gets the hash into the format we like it
// This allows an external tool to calculate the sum
func ReturnChecksumString(h hash.Hash) string {
	return base64.StdEncoding.WithPadding(base64.NoPadding).EncodeToString([]byte(h.Sum(nil)))
}

// CalcMd5File calculates the checksum for a specified filename
func CalcMd5File(directory, fn string) (string, error) {
	fp := directory + "/" + fn
	fmt.Println("Calculating Checksum for", fp)
	f, err := os.Open(fp)
	if err != nil {
		return "", err
	}
	defer f.Close()
	var h hash.Hash
	h = md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return ReturnChecksumString(h), nil
}

// Calculator is useful where we get streams of bytes in (e.g. from the network)
// We expose an io.Writer
// close the trigger chanel then wait for the writes to finish
func Calculator(fp string) (iw io.Writer, trigger chan struct{}, wg sync.WaitGroup) {
	trigger = make(chan struct{})
	var h hash.Hash
	h = md5.New()
	wg.Add(1)

	go func() {
		directory, fn := filepath.Split(fp)
		dm := DirectoryMapFromDir(directory)
		<-trigger
		fs := FsFromName(directory, fn)
		fs.Checksum = ReturnChecksumString(h)
		dm.Add(fs)
		dm.WriteDirectory(directory)
		wg.Done()
	}()
	iw = io.Writer(h)
	return
}
