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

// MdConfig structure used to specify the detailed config
type MdConfig struct {
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

// NewMdConfig reads the config from an xml file
// Returns error if file exists but cannot be read or parsed
func NewMdConfig(fn string) (*MdConfig, error) {
	itm := new(MdConfig)
	itm.fn = fn
	var f *os.File
	_, err := os.Stat(fn)

	if !os.IsNotExist(err) {
		f, err = os.Open(fn)
		if err != nil {
			return nil, fmt.Errorf("error opening MdConfig file: %w", err)
		}
		byteValue, err := io.ReadAll(f)
		_ = f.Close()
		if err != nil {
			return nil, fmt.Errorf("error loading MdConfig file: %w", err)
		}
		err = itm.FromXML(byteValue)
		if err != nil {
			return nil, fmt.Errorf("unable to unmarshal config: %w", err)
		}
	}
	return itm, nil
}

// LoadOrCreateMdConfig loads the config from the default location or creates it if it doesn't exist
// This is the recommended way to get MdConfig in commands
func LoadOrCreateMdConfig() (*MdConfig, error) {
	return LoadOrCreateMdConfigWithPath("")
}

// LoadOrCreateMdConfigWithPath loads the config from the specified path or default location
// If configPath is empty, uses default behavior (XmConfig or ~/.medorg.xml)
// If configPath is provided, uses that path directly
func LoadOrCreateMdConfigWithPath(configPath string) (*MdConfig, error) {
	// If a specific path is provided, use it
	if configPath != "" {
		return NewMdConfig(configPath)
	}

	// First check if XmConfig returns a location (looks for existing .medorg.xml)
	if xmcf := XmConfig(); xmcf != "" {
		return NewMdConfig(string(xmcf))
	}

	// If not found, use default location: ~/.medorg.xml
	fn := ConfigPath(".medorg.xml")
	return NewMdConfig(fn)
}

func (xc *MdConfig) WriteXmlCfg() error {
	data, err := xml.MarshalIndent(xc, "", "  ")
	if err != nil {
		return err
	}
	err = os.WriteFile(xc.fn, data, 0o600)
	return err
}

// FromXML populate from an ba
func (xc *MdConfig) FromXML(input []byte) (err error) {
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

func (xc *MdConfig) HasLabel(label string) bool {
	for _, v := range xc.VolumeLabels {
		if label == v {
			return true
		}
	}
	return false
}

// Add a volume label
// returns false if the label already exists
func (xc *MdConfig) AddLabel(label string) bool {
	if xc.HasLabel(label) {
		return false
	}
	fmt.Println("Adding Label", label)
	xc.VolumeLabels = append(xc.VolumeLabels, label)
	return true
}

// HasSourceDirectory checks if a source directory with the given alias exists
func (xc *MdConfig) HasSourceDirectory(alias string) bool {
	for _, sd := range xc.SourceDirectories {
		if sd.Alias == alias {
			return true
		}
	}
	return false
}

// AddSourceDirectory adds a new source directory with an alias
// Returns false if the alias already exists
func (xc *MdConfig) AddSourceDirectory(path, alias string) bool {
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
func (xc *MdConfig) RemoveSourceDirectory(alias string) bool {
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
func (xc *MdConfig) GetSourceDirectory(alias string) (SourceDirectory, bool) {
	for _, sd := range xc.SourceDirectories {
		if sd.Alias == alias {
			return sd, true
		}
	}
	return SourceDirectory{}, false
}

// GetSourcePaths returns all configured source directory paths
func (xc *MdConfig) GetSourcePaths() []string {
	paths := make([]string, len(xc.SourceDirectories))
	for i, sd := range xc.SourceDirectories {
		paths[i] = sd.Path
	}
	return paths
}

// GetAliasForPath returns the alias for a given path
// Returns empty string if not found
func (xc *MdConfig) GetAliasForPath(path string) string {
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
func (xc *MdConfig) SetRestoreDestination(alias, path string) error {
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
func (xc *MdConfig) GetRestoreDestination(alias string) (string, bool) {
	for _, rd := range xc.RestoreDestinations {
		if rd.Alias == alias {
			return rd.Path, true
		}
	}
	return "", false
}

// RemoveRestoreDestination removes a restore destination by alias
func (xc *MdConfig) RemoveRestoreDestination(alias string) bool {
	for i, rd := range xc.RestoreDestinations {
		if rd.Alias == alias {
			xc.RestoreDestinations = append(xc.RestoreDestinations[:i], xc.RestoreDestinations[i+1:]...)
			return true
		}
	}
	return false
}
