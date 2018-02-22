package medorg

import (
	"encoding/xml"
	"io"
	"io/ioutil"
	"log"
	"os"
)

// XMLCfg structure used to specify the detailed config
type XMLCfg struct {
	XMLName struct{} `xml:"xc"`
	Af      []string `xml:"af"`
}

// NewXMLCfg reads the config from an xml file
func NewXMLCfg(fn string) *XMLCfg {
	itm := new(XMLCfg)
	var f *os.File
	_, err := os.Stat(fn)

	if !os.IsNotExist(err) {
		f, err = os.Open(fn)

		if err != nil {
			log.Fatalf("error opening NewXMLCfg file: %T,%v\n", err, err)
		}
		byteValue, err := ioutil.ReadAll(f)
		_ = f.Close()
		if err != nil {
			log.Fatalf("error loading NewXMLCfg file: %T,%v\n", err, err)
		}
		err = itm.FromXML(byteValue)
		if err != nil {
			log.Fatal("Unable to unmarshal config, NewXMLCfg", err)
		}
	}
	return itm
}

// FromXML populate from an ba
func (xc *XMLCfg) FromXML(input []byte) (err error) {
	err = xml.Unmarshal(input, xc)
	//fmt.Printf("Unmarshalling completed on:\n%v\nOutput:\n%v\n\n",input, xc)
	switch err {
	case nil:
	case io.EOF:
		err = nil
	default:
		log.Fatal("Unknown Error UnMarshalling Config:", err)
	}
	return
}
