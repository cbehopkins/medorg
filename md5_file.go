package medorg

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
)

// Md5File is the struct written into each directory
// It contains a lost of the files and the properties assoxciated with them
type Md5File struct {
	XMLName struct{}        `xml:"dr"`
	Ts      int64           `xml:"tstamp,attr,omitempty"`
	Dir     string          `xml:"dir,attr,omitempty"`
	Files   FileStructArray `xml:"fr"`
}

// NewMd5File creates a new one
func NewMd5File() *Md5File {
	itm := new(Md5File)
	// itm.Files = make([]FileStruct, 0)
	return itm
}

// append Adds a struct to the struct
func (md *Md5File) append(fs FileStruct) {
	md.Files = append(md.Files, fs)
}

// AddFile adds a file to the struct
// func (md *Md5File) AddFile(filename string) {
// 	md.append(FileStruct{Name: filename})
// }

// ToXML standard marshaller
func (md Md5File) ToXML() ([]byte, error) {
	return xml.MarshalIndent(md, "", "  ")
}
func (md Md5File) String() string {
	txt, err := xml.MarshalIndent(md, "", "  ")
	switch err {
	case nil:
	case io.EOF:
	default:
		log.Fatal("Unknown Error Marshalling Md5File:", err)
	}
	return string(txt)
}

// FromXML Standard unmarshaller
func (md *Md5File) FromXML(input []byte) (err error) {
	err = xml.Unmarshal(input, md)
	//fmt.Printf("Unmarshalling completed on:\n%v\nOutput:\n%v\n\n",input, md)
	xse := &xml.SyntaxError{}
	switch true {
	case err == nil:
	case errors.Is(err, io.EOF):
		err = nil
	case errors.As(err, &xse):
		log.Println("Unmarshalling error:", err)
		err = nil
	default:
		return fmt.Errorf("unknown Error UnMarshalling Md5File:%w", err)
	}
	return nil
}
func (md Md5File) Sort() {
	sort.Sort(md.Files)
}
func (md0 Md5File) Equal(md1 Md5File) bool {
	if md0.Dir != md1.Dir {
		return false
	}
	if len(md0.Files) != len(md1.Files) {
		return false
	}
	md0.Sort()
	md1.Sort()

	for i, v := range md0.Files {
		// We also care about the name being the same
		if v.Name != md1.Files[i].Name {
			return false
		}
		// The default equals looks at things like checksim and size.
		if !v.Equal(md1.Files[i]) {
			return false
		}
	}
	return true
}
