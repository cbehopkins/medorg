package core

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
)

// Md5File is the struct written into each directory
// It contains a list of the files and the properties associated with them
type Md5File struct {
	XMLName struct{}        `xml:"dr"`
	Dir     string          `xml:"dir,attr,omitempty"`
	Alias   string          `xml:"alias,attr,omitempty"` // Source directory alias for journal entries
	Files   FileStructArray `xml:"fr"`
}

// append adds a struct to the struct
func (md *Md5File) append(fs FileStruct) {
	md.Files = append(md.Files, fs)
}

func supressXmlUnmarshallErrors(err error) error {
	xse := &xml.SyntaxError{}
	switch true {
	case err == nil:
	case errors.Is(err, io.EOF):
		return nil
	case errors.As(err, &xse):
		// Suppress error from causing a genuine failure (disks are unreliable)
		// But still note that it happened
		log.Println("Unmarshalling error:", err)
	default:
		return fmt.Errorf("unknown Error UnMarshalling:%w", err)
	}
	return nil
}
