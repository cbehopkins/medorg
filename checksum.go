package medorg

import (
	"encoding/xml"
	"log"
	"sync"
)

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

type FileStruct struct {
	directory string // Kept as hidden from the xml as this is used for messaging between agents
	// and that does not want to end up in the final xml file
	delete   bool   // Should this file be deleted from the structure
	Name     string `xml:"name"`
	Checksum string `xml:"chk"`
}
type Md5File struct {
	Files []FileStruct `xml:"file"`
}

func NewMd5File() *Md5File {
	itm := new(Md5File)
	itm.Files = make([]FileStruct, 0)
	return itm
}
func (md *Md5File) AddFile(filename string) {
	md.Files = append(md.Files, FileStruct{Name: filename})
	// TBD addin code to start the checksum process
}
func (md Md5File) MarshalXml() (output []byte, err error) {
	output, err = xml.Marshal(md)
	return
}
func (md Md5File) String() string {
	txt, err := md.MarshalXml()
	check(err)
	return string(txt)
}
func (md *Md5File) UnmarshalXml(input string) (err error) {
	input_bytes := []byte(input)
	err = xml.Unmarshal(input_bytes, md)
	check(err)
	return
}
func NewChannels() (input_chan chan FileStruct, output_chan chan FileStruct, closed_chan chan struct{}) {
	input_chan = make(chan FileStruct)
	output_chan = make(chan FileStruct)
	closed_chan = make(chan struct{})
	go md5Calcer(input_chan, output_chan, closed_chan)
	return
}

func md5Calcer(input_chan chan FileStruct, output_chan chan FileStruct, closed_chan chan struct{}) {
	for itm := range input_chan {
		// TBD calculate the MD5 here and send it
		output_chan <- itm
	}
	close(output_chan)
	close(closed_chan)
}
