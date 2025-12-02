package core

import (
	"io/fs"
)

// FileVisitor defines a function that visits files in a directory
// This provides an abstraction over the visitor pattern, allowing
// different implementations without tight coupling to concrete types
type FileVisitor interface {
	// Visit is called for each file in a directory
	// Returns an error if the visit should be aborted
	Visit(metadata FileMetadata, fileInfo fs.FileInfo) error
}

// ExtendedDirectoryVisitor processes files with full directory context
// This is more detailed than the DirectoryVisitor in types.go
type ExtendedDirectoryVisitor interface {
	// Visit is called for each file with directory and entry information
	Visit(storage DirectoryStorage, dir, filename string, entry fs.DirEntry, metadata FileMetadata, fileInfo fs.FileInfo) error
}

// SimpleFileVisitor wraps a simple function into a FileVisitor interface
type SimpleFileVisitor func(metadata FileMetadata, fileInfo fs.FileInfo) error

func (f SimpleFileVisitor) Visit(metadata FileMetadata, fileInfo fs.FileInfo) error {
	return f(metadata, fileInfo)
}

// SimpleDirVisitor wraps a simple function into an ExtendedDirectoryVisitor interface
type SimpleDirVisitor func(storage DirectoryStorage, dir, filename string, entry fs.DirEntry, metadata FileMetadata, fileInfo fs.FileInfo) error

func (f SimpleDirVisitor) Visit(storage DirectoryStorage, dir, filename string, entry fs.DirEntry, metadata FileMetadata, fileInfo fs.FileInfo) error {
	return f(storage, dir, filename, entry, metadata, fileInfo)
}

// LegacyVisitorAdapter adapts the old visitor function signature to ExtendedDirectoryVisitor
type LegacyVisitorAdapter struct {
	legacyFunc func(dm DirectoryMap, dir, fn string, d fs.DirEntry, fileStruct FileStruct, fileInfo fs.FileInfo) error
}

func (lva *LegacyVisitorAdapter) Visit(storage DirectoryStorage, dir, filename string, entry fs.DirEntry, metadata FileMetadata, fileInfo fs.FileInfo) error {
	// Convert storage back to *DirectoryMap for legacy compatibility
	dm, ok := storage.(*DirectoryMap)
	if !ok {
		// If not a DirectoryMap, we can't call the legacy function
		// This shouldn't happen if used correctly
		return nil
	}

	// Convert metadata back to *FileStruct for legacy compatibility
	fs, ok := metadata.(*FileStruct)
	if !ok {
		// If not a FileStruct, we can't call the legacy function
		return nil
	}

	return lva.legacyFunc(*dm, dir, filename, entry, *fs, fileInfo)
}

// NewLegacyVisitorAdapter creates an adapter for old-style visitor functions
func NewLegacyVisitorAdapter(
	legacyFunc func(dm DirectoryMap, dir, fn string, d fs.DirEntry, fileStruct FileStruct, fileInfo fs.FileInfo) error,
) ExtendedDirectoryVisitor {
	return &LegacyVisitorAdapter{legacyFunc: legacyFunc}
}
