package core

import (
	"errors"
	"io"
	"log"
)

// Md5File is the struct written into each directory
// It contains a list of the files and the properties associated with them
type Md5File struct {
	XMLName struct{}        `xml:"dr"`
	Dir     string          `xml:"dir,attr,omitempty"`
	Files   FileStructArray `xml:"fr"`
}

// append adds a struct to the struct
func (md *Md5File) append(fs FileStruct) {
	md.Files = append(md.Files, fs)
}

func supressXmlUnmarshallErrors(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, io.EOF) {
		return nil
	}
	// For robustness: treat any XML unmarshal error as recoverable.
	// Disks and metadata can be flaky; we can rebuild from the filesystem.
	// Log for observability but don't fail the run.
	log.Println("Unmarshalling error:", err)
	return nil
}
