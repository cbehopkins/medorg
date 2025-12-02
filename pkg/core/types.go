package core

import (
	"io/fs"
)

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

// ChecksumCalculator handles MD5 checksum operations
type ChecksumCalculator interface {
	// Calculate computes the checksum for a file
	Calculate(directory, filename string) (string, error)
	// Validate checks if the stored checksum matches the file
	Validate(fm FileMetadata) error
	// Update recalculates and updates the checksum if needed
	Update(fm FileMetadata, forceUpdate bool) error
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

// DirectoryVisitor processes files in a directory
type DirectoryVisitor interface {
	// Visit is called for each file in a directory
	Visit(directory, filename string, entry fs.DirEntry) error
}

// DirectoryTraverser walks directory trees
type DirectoryTraverser interface {
	// Start begins the traversal
	Start() error
	// Close stops the traversal and cleans up
	Close()
	// ErrChan returns a channel for receiving errors during traversal
	ErrChan() <-chan error
	// VisitFile queues a file to be visited
	VisitFile(dir, file string, d fs.DirEntry, callback func())
}

// BackupManager handles backup operations
type BackupManager interface {
	// ScanSource scans the source directory for files
	ScanSource(directory string) error
	// ScanDestination scans the destination directory
	ScanDestination(directory string) error
	// FindDuplicates identifies files that exist in both source and destination
	FindDuplicates() ([]FileMetadata, error)
	// GenerateCopyList creates a prioritized list of files to copy
	GenerateCopyList() ([]FileMetadata, error)
	// CopyFiles performs the actual file copying
	CopyFiles(copyFunc func(src, dst string) error) error
}

// VolumeManager handles volume label operations
type VolumeManager interface {
	// GetLabel returns the volume label for a directory
	GetLabel(directory string) (string, error)
	// SetLabel assigns a volume label to a directory
	SetLabel(directory, label string) error
	// FindVolume searches up the directory tree for a volume configuration
	FindVolume(directory string) (interface{}, error)
}

// ConfigurationManager handles application configuration
type ConfigurationManager interface {
	// Load reads configuration from file
	Load(configPath string) error
	// Save writes configuration to file
	Save() error
	// GetVolume returns volume configuration by label
	GetVolume(label string) (interface{}, error)
	// AddVolume adds a new volume configuration
	AddVolume(vc interface{}) error
}

// JournalManager handles directory change journaling
type JournalManager interface {
	// RecordVisit records a directory visit
	RecordVisit(de interface{}, dir string) error
	// RecordAdd records a file addition
	RecordAdd(de interface{}, dir string) error
	// RecordModify records a file modification
	RecordModify(de interface{}, dir string) error
	// RecordDelete records a file deletion
	RecordDelete(de interface{}, dir string) error
	// ValidateJournal checks journal consistency
	ValidateJournal() error
}

// FileOperations provides low-level file operations
type FileOperations interface {
	// Copy copies a file from source to destination
	Copy(src, dst string) error
	// Move moves a file from source to destination
	Move(src, dst string) error
	// Delete removes a file
	Delete(path string) error
	// Exists checks if a file exists
	Exists(path string) bool
}

// DuplicateDetector finds duplicate files
type DuplicateDetector interface {
	// AddFile adds a file to the duplicate detection index
	AddFile(fm FileMetadata)
	// FindDuplicates returns groups of duplicate files
	FindDuplicates() [][]FileMetadata
	// IsDuplicate checks if a file has duplicates
	IsDuplicate(fm FileMetadata) bool
}

// MoveDetector detects moved files based on name and size
type MoveDetector interface {
	// AddMissing adds a file that's missing from filesystem but in metadata
	AddMissing(fm FileMetadata)
	// AddNew adds a file that exists in filesystem but not in metadata
	AddNew(entry fs.DirEntry) (FileMetadata, error)
	// FindMoves matches missing files with new files
	FindMoves() map[string]string
}

// FilePrioritizer determines backup priority for files
type FilePrioritizer interface {
	// Prioritize sorts files by backup priority
	Prioritize(files []FileMetadata, volumeLabel string) []FileMetadata
	// CalculatePriority computes priority score for a file
	CalculatePriority(fm FileMetadata) int
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
