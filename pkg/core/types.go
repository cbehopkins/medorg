package core

// Core Interfaces - Define the contracts between different parts of the system
// These interfaces establish clear boundaries and allow for better decoupling

// FileMetadata represents the essential metadata for a file
// This interface describes the contract for file metadata operations
// FileStruct is the primary implementation of this interface
type FileMetadata interface {
	PathProvider
	SizeProvider
	ChecksumProvider
	TagManager
	BackupTracker
}

// DirectoryStorage manages persistence of directory metadata
type DirectoryStorage interface {
	// Load reads metadata from storage (e.g., .medorg.xml)
	Load(directory string) error
	// Save writes metadata to storage
	Save(directory string) error
	// GetFile retrieves metadata for a specific file
	GetFile(filename string) (FileMetadata, error)
	// AddFile adds or updates file metadata
	AddFile(fm FileMetadata) error
	// RemoveFile removes file metadata
	RemoveFile(filename string) error
	// ListFiles returns all files in the directory
	ListFiles() []FileMetadata
}

// Core Data Structure Interfaces
// These define the minimum contract for data structures used throughout the system

// PathProvider provides file path information
type PathProvider interface {
	// Directory returns the directory containing the file
	Directory() string
	// Path returns the full file path
	Path() Fpath
	// GetName returns the filename
	GetName() string
}

// SizeProvider provides file size information
type SizeProvider interface {
	// GetSize returns the file size in bytes
	GetSize() int64
}

// ChecksumProvider provides checksum information
type ChecksumProvider interface {
	// GetChecksum returns the file checksum
	GetChecksum() string
}

// TagManager manages file tags
type TagManager interface {
	// HasTag checks if a tag exists
	HasTag(tag string) bool
	// AddTag adds a tag to the file
	AddTag(tag string) bool
	// RemoveTag removes a tag from the file
	RemoveTag(tag string) bool
	// GetTags returns a copy of all tags
	GetTags() []string
}

// BackupTracker tracks backup locations
type BackupTracker interface {
	// BackupDestinations returns all backup volume labels where this file exists
	BackupDestinations() []string
	// AddBackupDestination adds a backup volume label
	AddBackupDestination(label string)
	// HasBackupOn checks if backed up to a specific volume
	HasBackupOn(label string) bool
}
