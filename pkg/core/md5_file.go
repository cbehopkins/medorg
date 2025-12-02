package core

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
)

// Md5File is the struct written into each directory
// It contains a lost of the files and the properties assoxciated with them
type Md5File struct {
	XMLName struct{}        `xml:"dr"`
	Dir     string          `xml:"dir,attr,omitempty"`
	Files   FileStructArray `xml:"fr"`
}

// append adds a struct to the struct
func (md *Md5File) append(fs FileStruct) {
	md.Files = append(md.Files, fs)
}

// func (md Md5File) String() string {
// 	txt, err := xml.MarshalIndent(md, "", "  ")
// 	switch err {
// 	case nil:
// 	case io.EOF:
// 	default:
// 		log.Fatal("Unknown Error Marshalling Md5File:", err)
// 	}
// 	return string(txt)
// }
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

// Equal compares to Md5Files and returns true if they are equal
// func (md0 Md5File) Equal(md1 Md5File) bool {
// 	if md0.Dir != md1.Dir {
// 		return false
// 	}
// 	if len(md0.Files) != len(md1.Files) {
// 		return false
// 	}

// 	sort.Sort(md0.Files)
// 	sort.Sort(md1.Files)

// 	for i, v := range md0.Files {
// 		// We also care about the name being the same
// 		if v.Name != md1.Files[i].Name {
// 			return false
// 		}
// 		// The default equals looks at things like checksim and size.
// 		if !v.Equal(md1.Files[i]) {
// 			return false
// 		}
// 	}
// 	return true
// }
