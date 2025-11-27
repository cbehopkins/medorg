package medorg

import (
	"fmt"
)

// Md5Calculator implements the ChecksumCalculator interface
// It provides MD5-based checksum operations for files
type Md5Calculator struct{}

// NewMd5Calculator creates a new MD5 checksum calculator
func NewMd5Calculator() *Md5Calculator {
	return &Md5Calculator{}
}

// Calculate computes the MD5 checksum for a file (implements ChecksumCalculator.Calculate)
func (mc *Md5Calculator) Calculate(directory, filename string) (string, error) {
	return CalcMd5File(directory, filename)
}

// Validate checks if the stored checksum matches the file (implements ChecksumCalculator.Validate)
func (mc *Md5Calculator) Validate(fm FileMetadata) error {
	// We need to access FileStruct directly to get the checksum and validate
	fs, ok := fm.(*FileStruct)
	if !ok {
		return fmt.Errorf("FileMetadata is not a *FileStruct")
	}
	return fs.ValidateChecksum()
}

// Update recalculates and updates the checksum if needed (implements ChecksumCalculator.Update)
func (mc *Md5Calculator) Update(fm FileMetadata, forceUpdate bool) error {
	// We need to access FileStruct directly to update the checksum
	fs, ok := fm.(*FileStruct)
	if !ok {
		return fmt.Errorf("FileMetadata is not a *FileStruct")
	}
	return fs.UpdateChecksum(forceUpdate)
}
