package core

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Bunch of stuff to manage the backup label on the target drive's side
// Elsewhere we talk about ArchivedAt, so here we need to have the target drive side stuff
// to manage this
// That means we need a way to generate brand new files for a drive
// and a file on the target to say what we have chosen
// Finally we need to update the user's master config file with a list of those we have historically picked
// just so that we don't accidentally (very improbable) resuse the same label

type VolumeCfg struct {
	XMLName struct{} `xml:"vol"`
	Label   string   `xml:"label"`
	fn      string
}

// NewVolumeCfg reads the config from an xml file
func NewVolumeCfg(xc *XMLCfg, fn string) (*VolumeCfg, error) {
	itm := new(VolumeCfg)
	var f *os.File
	_, err := os.Stat(fn)
	itm.fn = fn

	if os.IsNotExist(err) {
		err := itm.GenerateNewVolumeLabel(xc)
		if err != nil {
			return nil, err
		}
	} else {
		f, err = os.Open(fn)
		if err != nil {
			return nil, fmt.Errorf("error opening NewVolumeCfg file:%s::%w", fn, err)
		}
		defer f.Close()

		byteValue, err := ioutil.ReadAll(f)
		if err != nil {
			return nil, fmt.Errorf("error loading NewVolumeCfg file:%s::%w", fn, err)
		}
		err = itm.FromXML(byteValue)
		if err != nil {
			return nil, fmt.Errorf("unable to unmarshal config NewVolumeCfg file:%s::%w", fn, err)
		}
	}
	// We don't care if the label is there already or not
	_ = xc.AddLabel(itm.Label)
	return itm, nil
}

// FromXML populate from a ba
func (vc *VolumeCfg) FromXML(input []byte) (err error) {
	err = xml.Unmarshal(input, vc)
	switch err {
	case nil:
	case io.EOF:
		err = nil
	default:
		return fmt.Errorf("unknown Error UnMarshalling Config:%w", err)
	}
	return
}

// Chunk of code nicked from stackoverflow
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var src = rand.NewSource(time.Now().UnixNano())

// RandStringBytesMaskImprSrcSB form a random string of length n
func RandStringBytesMaskImprSrcSB(n int) string {
	sb := strings.Builder{}
	sb.Grow(n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			sb.WriteByte(letterBytes[idx])
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return sb.String()
}

// ToXML standard marshaller
func (vc VolumeCfg) ToXML() (output []byte, err error) {
	output, err = xml.MarshalIndent(vc, "", "  ")
	return
}

// Persist the struct to disk
func (vc VolumeCfg) Persist() error {
	fn := vc.fn
	if fn == "" {
		return errors.New("missing Volume Config filename")
	}
	output, err := vc.ToXML()
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(fn, output, 0o600)
	if err != nil {
		return err
	}
	return nil
}

func (vc *VolumeCfg) GenerateNewVolumeLabel(xc *XMLCfg) error {
	for {
		vc.Label = RandStringBytesMaskImprSrcSB(8)
		if xc.AddLabel(vc.Label) {
			return vc.Persist()
		}
	}
}

func formVolumeName(dir string) string {
	return filepath.Join(dir, ".mdbackup.xml")
}

func findVolumeConfig(dir string) string {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return ""
	}
	info, err := os.Stat(dir)
	if err != nil {
		return ""
	}
	if !info.IsDir() {
		dir, _ = filepath.Split(dir)
	}

	finder := func(d string) bool {
		fn := filepath.Join(filepath.VolumeName(dir), formVolumeName(d))
		_, err := os.Stat(fn)
		return !errors.Is(err, os.ErrNotExist)
	}
	getParent := func(dir string) string {
		ds := strings.Split(dir, string(filepath.Separator))
		if len(ds) > 1 {
			result := filepath.Join(ds[0 : len(ds)-1]...)
			if strings.HasPrefix(dir, string(filepath.Separator)) {
				return filepath.Join(string(filepath.Separator), result)
			}
			return result
		}
		return string(filepath.Separator)
	}

	d := strings.TrimPrefix(dir, filepath.VolumeName(dir))
	for exists := finder(d); !exists; exists = finder(d) {
		d = getParent(d)
		if d == string(filepath.Separator) {
			return formVolumeName(dir)
		}
	}
	return filepath.Join(filepath.VolumeName(dir), formVolumeName(d))
}

// VolumeCfgFromDir get volume config appropriate for the requested directory
func (xc *XMLCfg) VolumeCfgFromDir(dir string) (*VolumeCfg, error) {
	fn := findVolumeConfig(dir)
	vc, err := NewVolumeCfg(xc, fn)
	return vc, err
}

// FIXME - this is not needed
func (xc *XMLCfg) getVolumeLabel(destDir string) (string, error) {
	vc, err := xc.VolumeCfgFromDir(destDir)
	if err != nil {
		return "", err
	}
	return vc.Label, err
}
