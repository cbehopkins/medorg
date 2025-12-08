/*
Package main implements mdlabel - a tool to manage volume labels for backup destinations.

Overview:
mdlabel creates and displays volume labels (.mdbackup.xml files) at the root of backup
destination directories. These labels uniquely identify backup volumes (typically external
drive mount points) and are used by other medorg tools to track which files have been
backed up to which destinations.

Usage:

	mdlabel <command> <path>

Commands:

	create <path>   Create a new volume label at the specified directory
	                Generates a unique 8-character label and writes .mdbackup.xml
	                Error if label already exists

	show <path>     Display the volume label for the specified directory
	                Searches upward from path to find .mdbackup.xml
	                Error if no label found

	recreate <path> Force creation of a new volume label, replacing any existing one
	                Warning: This will orphan any backup metadata referencing the old label

Example:

	mdlabel create E:/
	mdlabel show E:/MyBackups
	mdlabel recreate F:/
*/
package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cbehopkins/medorg/pkg/cli"
	"github.com/cbehopkins/medorg/pkg/core"
)

func main() {
	cli.ExitFromRun(run())
}

// Config holds the configuration for mdlabel operations
type Config struct {
	Command string
	Path    string
	Stdout  io.Writer
}

// run executes the mdlabel command and returns an exit code and error
func run() (int, error) {
	if len(os.Args) < 3 {
		printUsage()
		return cli.ExitInvalidArgs, nil
	}

	command := os.Args[1]
	path := os.Args[2]

	// Validate path exists
	if err := cli.ValidatePath(path, false); err != nil {
		return cli.ExitPathNotExist, fmt.Errorf("path '%s' does not exist", path)
	}

	xc := &core.MdConfig{}

	switch command {
	case "create":
		if err := createLabel(xc, path); err != nil {
			return cli.ExitConfigError, err
		}

	case "show":
		if err := showLabel(xc, path); err != nil {
			return cli.ExitNoVolumeLabel, err
		}

	case "recreate":
		if err := recreateLabel(xc, path); err != nil {
			return cli.ExitConfigError, err
		}

	default:
		printUsage()
		return cli.ExitInvalidArgs, fmt.Errorf("unknown command: %s", command)
	}

	return cli.ExitOk, nil
}

func createLabel(xc *core.MdConfig, path string) error {
	// Check if label file already exists (don't use GetVolumeLabel as it creates one)
	labelFile := filepath.Join(path, ".mdbackup.xml")
	if _, err := os.Stat(labelFile); err == nil {
		return fmt.Errorf("volume label already exists at %s (use 'recreate' to replace)", path)
	}

	// Create new volume label
	vc, err := xc.VolumeCfgFromDir(path)
	if err != nil {
		return fmt.Errorf("failed to create volume label: %w", err)
	}

	fmt.Printf("✓ Created volume label: %s\n", vc.Label)
	fmt.Printf("  Location: %s\n", path)
	return nil
}

func showLabel(xc *core.MdConfig, path string) error {
	label, err := xc.GetVolumeLabel(path)
	if err != nil {
		return fmt.Errorf("no volume label found: %w", err)
	}

	fmt.Printf("Volume Label: %s\n", label)
	fmt.Printf("Path: %s\n", path)
	return nil
}

func recreateLabel(xc *core.MdConfig, path string) error {
	// Show existing label if present
	existingLabel, err := xc.GetVolumeLabel(path)
	if err == nil {
		fmt.Printf("⚠ Warning: Replacing existing label '%s'\n", existingLabel)
		fmt.Printf("  This will orphan backup metadata referencing the old label.\n")
	}

	// Force create new label by directly creating VolumeCfg
	vc, err := xc.VolumeCfgFromDir(path)
	if err != nil {
		return fmt.Errorf("failed to access volume config: %w", err)
	}

	// Generate and persist new label
	if err := vc.GenerateNewVolumeLabel(xc); err != nil {
		return fmt.Errorf("failed to generate new label: %w", err)
	}

	fmt.Printf("✓ Created new volume label: %s\n", vc.Label)
	fmt.Printf("  Location: %s\n", path)
	return nil
}

func printUsage() {
	fmt.Println("mdlabel - Manage volume labels for backup destinations")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  mdlabel create <path>     Create a new volume label")
	fmt.Println("  mdlabel show <path>       Display the volume label")
	fmt.Println("  mdlabel recreate <path>   Replace existing label (warning: orphans metadata)")
	fmt.Println()
	fmt.Println("Example:")
	fmt.Println("  mdlabel create E:/")
	fmt.Println("  mdlabel show E:/MyBackups")
}
