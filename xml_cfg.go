package medorg

import (
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
)

// XMLCfg structure used to specify the detailed config
type XMLCfg struct {
	XMLName struct{} `xml:"xc"`

	// Autoformatting rules
	Af []string `xml:"af"`
	// Volume Labels we have encountered
	VolumeLabels []string `xml:"vl"`

	fn string
}

// NewXMLCfg reads the config from an xml file
func NewXMLCfg(fn string) *XMLCfg {
	itm := new(XMLCfg)
	itm.fn = fn
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

// WriteXMLCfg writes the config to an xml file
func (xc *XMLCfg) WriteXMLCfg() error {
	data, err := xml.MarshalIndent(xc, "", "  ")
	if err != nil {
		return err
	}
	err = os.WriteFile(xc.fn, data, 0600)
	return err
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

// HasLabel checks if a label exists
func (xc *XMLCfg) HasLabel(label string) bool {
	for _, v := range xc.VolumeLabels {
		if label == v {
			return true
		}
	}
	return false
}

// AddLabel Add a volume label
// returns false if the label already exists
func (xc *XMLCfg) AddLabel(label string) bool {
	if xc.HasLabel(label) {
		return false
	}
	fmt.Println("Adding Label", label)
	xc.VolumeLabels = append(xc.VolumeLabels, label)
	return true
}
