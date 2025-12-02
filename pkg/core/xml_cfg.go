package core

import (
	"encoding/xml"
	"fmt"
	"io"
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
// Returns error if file exists but cannot be read or parsed
func NewXMLCfg(fn string) (*XMLCfg, error) {
	itm := new(XMLCfg)
	itm.fn = fn
	var f *os.File
	_, err := os.Stat(fn)

	if !os.IsNotExist(err) {
		f, err = os.Open(fn)
		if err != nil {
			return nil, fmt.Errorf("error opening NewXMLCfg file: %w", err)
		}
		byteValue, err := io.ReadAll(f)
		_ = f.Close()
		if err != nil {
			return nil, fmt.Errorf("error loading NewXMLCfg file: %w", err)
		}
		err = itm.FromXML(byteValue)
		if err != nil {
			return nil, fmt.Errorf("unable to unmarshal config, NewXMLCfg: %w", err)
		}
	}
	return itm, nil
}

func (xc *XMLCfg) WriteXmlCfg() error {
	data, err := xml.MarshalIndent(xc, "", "  ")
	if err != nil {
		return err
	}
	err = os.WriteFile(xc.fn, data, 0o600)
	return err
}

// FromXML populate from an ba
func (xc *XMLCfg) FromXML(input []byte) (err error) {
	err = xml.Unmarshal(input, xc)
	// fmt.Printf("Unmarshalling completed on:\n%v\nOutput:\n%v\n\n",input, xc)
	switch err {
	case nil:
	case io.EOF:
		err = nil
	default:
		return fmt.Errorf("error unmarshalling config: %w", err)
	}
	return
}

func (xc *XMLCfg) HasLabel(label string) bool {
	for _, v := range xc.VolumeLabels {
		if label == v {
			return true
		}
	}
	return false
}

// Add a volume label
// returns false if the label already exists
func (xc *XMLCfg) AddLabel(label string) bool {
	if xc.HasLabel(label) {
		return false
	}
	fmt.Println("Adding Label", label)
	xc.VolumeLabels = append(xc.VolumeLabels, label)
	return true
}
