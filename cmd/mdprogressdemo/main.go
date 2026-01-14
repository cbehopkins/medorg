package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/cbehopkins/medorg/pkg/core"
	pb "github.com/cbehopkins/pb/v3"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: progressdemo <directory>")
		os.Exit(1)
	}

	dir := os.Args[1]

	fmt.Printf("Walking directory: %s\n", dir)

	// Create a pool and factory for Progressable sources
	pool := pb.NewPool()
	if err := pool.Start(); err != nil {
		log.Fatalf("Failed to start progress bar pool: %v", err)
	}
	defer func() {
		if err := pool.Stop(); err != nil {
			log.Printf("Failed to stop pool: %v", err)
		}
	}()

	factory := pb.NewPoolProgressFactory(pool)

	// Create the ProgressableDirectoryWalker and register its progress
	walker := core.NewProgressableDirectoryWalker(core.MakeTokenChan(4), dir)
	if err := factory.Register(walker.Progress); err != nil {
		log.Fatalf("Failed to register directory walker: %v", err)
	}

	// Set up the file visitor - simulate 1ms of work per file
	fileCount := 0
	walker.AddFileVisitor(func(name core.Fname, fm core.FileMetadata, fi os.FileInfo) error {
		fileCount++
		time.Sleep(1 * time.Millisecond) // Simulate work
		return nil
	})
	// Walk the directory (don't print before starting - pool needs clean output)
	if err := walker.Walk(dir); err != nil {
		log.Fatalf("Walk failed: %v", err)
	}

	// Wait for progress reporting to complete
	factory.Wg.Wait()
	// Stop the pool to clean up the progress bar display
	if err := pool.Stop(); err != nil {
		log.Printf("Failed to stop pool: %v", err)
	}
	log.Printf("Completed! Visited %d files across %d directories\n", fileCount, walker.Progress.Total())
}
