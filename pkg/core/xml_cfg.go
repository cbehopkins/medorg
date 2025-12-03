package core

import (
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// SourceDirectory represents a configured source directory with its alias
type SourceDirectory struct {
	Path  string `xml:"path,attr"`
	Alias string `xml:"alias,attr"`
}

// XMLCfg structure used to specify the detailed config
type XMLCfg struct {
	XMLName struct{} `xml:"xc"`

	// Autoformatting rules
	Af []string `xml:"af"`
	// Volume Labels we have encountered
	VolumeLabels []string `xml:"vl"`
	// Source directories for backup/journal operations
	SourceDirectories []SourceDirectory `xml:"src"`

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

// HasSourceDirectory checks if a source directory with the given alias exists
func (xc *XMLCfg) HasSourceDirectory(alias string) bool {
	for _, sd := range xc.SourceDirectories {
		if sd.Alias == alias {
			return true
		}
	}
	return false
}

// AddSourceDirectory adds a new source directory with an alias
// Returns false if the alias already exists
func (xc *XMLCfg) AddSourceDirectory(path, alias string) bool {
	if xc.HasSourceDirectory(alias) {
		return false
	}
	// Clean the path to ensure consistency
	cleanPath := filepath.Clean(path)
	xc.SourceDirectories = append(xc.SourceDirectories, SourceDirectory{
		Path:  cleanPath,
		Alias: alias,
	})
	return true
}

// RemoveSourceDirectory removes a source directory by alias
// Returns true if removed, false if not found
func (xc *XMLCfg) RemoveSourceDirectory(alias string) bool {
	for i, sd := range xc.SourceDirectories {
		if sd.Alias == alias {
			xc.SourceDirectories = append(xc.SourceDirectories[:i], xc.SourceDirectories[i+1:]...)
			return true
		}
	}
	return false
}

// GetSourceDirectory returns the source directory for a given alias
// Returns empty SourceDirectory if not found
func (xc *XMLCfg) GetSourceDirectory(alias string) (SourceDirectory, bool) {
	for _, sd := range xc.SourceDirectories {
		if sd.Alias == alias {
			return sd, true
		}
	}
	return SourceDirectory{}, false
}

// GetSourcePaths returns all configured source directory paths
func (xc *XMLCfg) GetSourcePaths() []string {
	paths := make([]string, len(xc.SourceDirectories))
	for i, sd := range xc.SourceDirectories {
		paths[i] = sd.Path
	}
	return paths
}

// GetAliasForPath returns the alias for a given path
// Returns empty string if not found
func (xc *XMLCfg) GetAliasForPath(path string) string {
	cleanPath := filepath.Clean(path)
	for _, sd := range xc.SourceDirectories {
		if sd.Path == cleanPath {
			return sd.Alias
		}
	}
	return ""
}
