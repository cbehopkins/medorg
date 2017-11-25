package medorg

import (
	"encoding/xml"
	"io"
	"log"
)

// Md5File is the struct written into each directory
// It contains a lost of the files and the properties assoxciated with them
type Md5File struct {
	XMLName struct{}     `xml:"dr"`
	Files   []FileStruct `xml:"fr"`
}

// NewMd5File creates a new one
func NewMd5File() *Md5File {
	itm := new(Md5File)
	itm.Files = make([]FileStruct, 0)
	return itm
}

// Append Adds a struct to the struct
func (md *Md5File) Append(fs FileStruct) {
	md.Files = append(md.Files, fs)
}

// AddFile adds a file to the struct
func (md *Md5File) AddFile(filename string) {
	md.Append(FileStruct{Name: filename})
}

// MarshalXML standard marshaller
func (md Md5File) MarshalXML() (output []byte, err error) {
	//output, err = xml.Marshal(md)

	output, err = xml.MarshalIndent(md, "", "  ")
	return
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

// UnmarshalXML Standard unmarshaller
func (md *Md5File) UnmarshalXML(input []byte) (err error) {
	err = xml.Unmarshal(input, md)
	//fmt.Printf("Unmarshalling completed on:\n%v\nOutput:\n%v\n\n",input, md)
	switch err {
	case nil:
	case io.EOF:
		err = nil
	default:
		log.Fatal("Unknown Error UnMarshalling Md5File:", err)
	}
	return
}
