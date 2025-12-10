package core

import (
	"crypto/md5"
	"encoding/base64"
	"errors"
	"hash"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

// Debug when true prevents making file changes during debugging
// Currently only used in test bench scenarios
var Debug bool

type DirectoryMapMod func(DirectoryMap, string)

// Md5FileName is the filename we use to save the data in
const (
	Md5FileName      = ".medorg.xml"
	ConfigFileName   = ".mdcfg.xml"
	AfConfigFileName = ".autofix"
	JournalPathName  = ".mdjournal.xml"
	VolumePathName   = ".mdbackup.xml"
)

func IsMetadataFile(fn string) bool {
	return fn == Md5FileName || fn == ConfigFileName || fn == AfConfigFileName || fn == JournalPathName
}

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
