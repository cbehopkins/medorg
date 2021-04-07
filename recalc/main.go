package main

import (
	"fmt"
	"io/fs"
	"os"

	"github.com/cbehopkins/medorg"
)

func main() {
	visitor := func(de medorg.DirectoryEntry, directory, file string, d fs.DirEntry) {
		_ = de.UpdateChecksum(file)
	}

	makerFunc := func(dir string) medorg.DirectoryTrackerInterface {
		return medorg.NewDirectoryEntry(dir, visitor)
	}
	errChan := medorg.NewDirTracker(".", makerFunc)

	for err := range errChan {
		fmt.Println("Error received on closing:", err)
		os.Exit(2)
	}
	fmt.Println("Finished walking")
}
