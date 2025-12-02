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
