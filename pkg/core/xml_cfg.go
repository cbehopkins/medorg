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

// RestoreDestination represents a configured restore destination for an alias
type RestoreDestination struct {
	Alias string `xml:"alias,attr"`
	Path  string `xml:"path,attr"`
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
	// Restore destinations mapping aliases to restore paths
	RestoreDestinations []RestoreDestination `xml:"restore"`

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

// LoadOrCreateXMLCfg loads the config from the default location or creates it if it doesn't exist
// This is the recommended way to get XMLCfg in commands
func LoadOrCreateXMLCfg() (*XMLCfg, error) {
	return LoadOrCreateXMLCfgWithPath("")
}

// LoadOrCreateXMLCfgWithPath loads the config from the specified path or default location
// If configPath is empty, uses default behavior (XmConfig or ~/.medorg.xml)
// If configPath is provided, uses that path directly
func LoadOrCreateXMLCfgWithPath(configPath string) (*XMLCfg, error) {
	// If a specific path is provided, use it
	if configPath != "" {
		return NewXMLCfg(configPath)
	}

	// First check if XmConfig returns a location (looks for existing .medorg.xml)
	if xmcf := XmConfig(); xmcf != "" {
		return NewXMLCfg(string(xmcf))
	}

	// If not found, use default location: ~/.medorg.xml
	fn := ConfigPath(".medorg.xml")
	return NewXMLCfg(fn)
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

// SetRestoreDestination sets or updates the restore destination for an alias
// If path is empty, uses the source directory path as default
func (xc *XMLCfg) SetRestoreDestination(alias, path string) error {
	// If no path provided, try to use the source directory path
	if path == "" {
		sd, ok := xc.GetSourceDirectory(alias)
		if !ok {
			return fmt.Errorf("alias '%s' not found in source directories", alias)
		}
		path = sd.Path
	}

	cleanPath := filepath.Clean(path)

	// Update existing or add new
	for i, rd := range xc.RestoreDestinations {
		if rd.Alias == alias {
			xc.RestoreDestinations[i].Path = cleanPath
			return nil
		}
	}

	// Add new restore destination
	xc.RestoreDestinations = append(xc.RestoreDestinations, RestoreDestination{
		Alias: alias,
		Path:  cleanPath,
	})
	return nil
}

// GetRestoreDestination returns the restore destination path for an alias
func (xc *XMLCfg) GetRestoreDestination(alias string) (string, bool) {
	for _, rd := range xc.RestoreDestinations {
		if rd.Alias == alias {
			return rd.Path, true
		}
	}
	return "", false
}

// RemoveRestoreDestination removes a restore destination by alias
func (xc *XMLCfg) RemoveRestoreDestination(alias string) bool {
	for i, rd := range xc.RestoreDestinations {
		if rd.Alias == alias {
			xc.RestoreDestinations = append(xc.RestoreDestinations[:i], xc.RestoreDestinations[i+1:]...)
			return true
		}
	}
	return false
}
