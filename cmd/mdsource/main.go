package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/cbehopkins/medorg/pkg/core"
)

const (
	ExitOk = iota
	ExitNoConfig
	ExitInvalidArgs
	ExitAliasExists
	ExitAliasNotFound
	ExitPathNotExist
	ExitRestoreSetError
)

func main() {
	os.Exit(run(os.Stdout))
}

// run executes the command and writes output to the provided writer.
// It returns an exit code.
func run(stdout io.Writer) int {
	// Global config path variable
	var configPath string

	// Command line argument processing
	addCmd := flag.NewFlagSet("add", flag.ExitOnError)
	addCmd.StringVar(&configPath, "config", "", "Path to config file")
	addPath := addCmd.String("path", "", "Directory path to add")
	addAlias := addCmd.String("alias", "", "Alias/shortcode for the directory")

	removeCmd := flag.NewFlagSet("remove", flag.ExitOnError)
	removeCmd.StringVar(&configPath, "config", "", "Path to config file")
	removeAlias := removeCmd.String("alias", "", "Alias to remove")

	listCmd := flag.NewFlagSet("list", flag.ExitOnError)
	listCmd.StringVar(&configPath, "config", "", "Path to config file")

	restoreCmd := flag.NewFlagSet("restore", flag.ExitOnError)
	restoreCmd.StringVar(&configPath, "config", "", "Path to config file")
	restoreAlias := restoreCmd.String("alias", "", "Alias to configure restore destination for")
	restorePath := restoreCmd.String("path", "", "Restore destination path (optional, defaults to source path)")

	if len(os.Args) < 2 {
		printUsageTo(stdout)
		return ExitInvalidArgs
	}

	// Parse subcommand to get config path before loading config
	switch os.Args[1] {
	case "add":
		if err := addCmd.Parse(os.Args[2:]); err != nil {
			fmt.Fprintln(stdout, "Error parsing add command:", err)
			return ExitInvalidArgs
		}
	case "remove":
		if err := removeCmd.Parse(os.Args[2:]); err != nil {
			fmt.Fprintln(stdout, "Error parsing remove command:", err)
			return ExitInvalidArgs
		}
	case "list":
		if err := listCmd.Parse(os.Args[2:]); err != nil {
			fmt.Fprintln(stdout, "Error parsing list command:", err)
			return ExitInvalidArgs
		}
	case "restore":
		if err := restoreCmd.Parse(os.Args[2:]); err != nil {
			fmt.Fprintln(stdout, "Error parsing restore command:", err)
			return ExitInvalidArgs
		}
	default:
		printUsageTo(stdout)
		return ExitInvalidArgs
	}

	// Load XMLCfg after parsing to get configPath
	xc, err := core.LoadOrCreateMdConfigWithPath(configPath)
	if err != nil {
		fmt.Fprintln(stdout, "Error loading config file:", err)
		return ExitNoConfig
	}

	// helper to persist config on success
	writeCfg := func() int {
		if err := xc.WriteXmlCfg(); err != nil {
			fmt.Fprintln(stdout, "Error while saving config file:", err)
			return ExitNoConfig
		}
		return ExitOk
	}

	// Execute subcommand
	switch os.Args[1] {
	case "add":
		if *addPath == "" || *addAlias == "" {
			fmt.Fprintln(stdout, "Error: both -path and -alias are required")
			addCmd.PrintDefaults()
			return ExitInvalidArgs
		}

		// Verify path exists
		if _, err := os.Stat(*addPath); os.IsNotExist(err) {
			fmt.Fprintf(stdout, "Error: path '%s' does not exist\n", *addPath)
			return ExitPathNotExist
		}

		// Add to config
		if !xc.AddSourceDirectory(*addPath, *addAlias) {
			fmt.Fprintf(stdout, "Error: alias '%s' already exists\n", *addAlias)
			return ExitAliasExists
		}

		fmt.Fprintf(stdout, "Added source directory: %s -> %s\n", *addAlias, *addPath)
		return writeCfg()

	case "remove":
		if *removeAlias == "" {
			fmt.Fprintln(stdout, "Error: -alias is required")
			removeCmd.PrintDefaults()
			return ExitInvalidArgs
		}

		if !xc.RemoveSourceDirectory(*removeAlias) {
			fmt.Fprintf(stdout, "Error: alias '%s' not found\n", *removeAlias)
			return ExitAliasNotFound
		}
		fmt.Fprintf(stdout, "Removed source directory with alias: %s\n", *removeAlias)
		return writeCfg()

	case "list":
		if len(xc.SourceDirectories) == 0 {
			fmt.Fprintln(stdout, "No source directories configured")
			return ExitOk
		}

		w := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ALIAS\tPATH")
		fmt.Fprintln(w, "-----\t----")
		for _, sd := range xc.SourceDirectories {
			fmt.Fprintf(w, "%s\t%s\n", sd.Alias, sd.Path)
		}
		w.Flush()
		return ExitOk

	case "restore":
		if *restoreAlias == "" {
			fmt.Fprintln(stdout, "Error: -alias is required")
			restoreCmd.PrintDefaults()
			return ExitInvalidArgs
		}

		// Set restore destination (empty path means use source path)
		if err := xc.SetRestoreDestination(*restoreAlias, *restorePath); err != nil {
			fmt.Fprintf(stdout, "Error: %v\n", err)
			return ExitRestoreSetError
		}

		destPath, _ := xc.GetRestoreDestination(*restoreAlias)
		if *restorePath == "" {
			fmt.Fprintf(stdout, "Configured restore destination for '%s' to default (source path): %s\n", *restoreAlias, destPath)
		} else {
			fmt.Fprintf(stdout, "Configured restore destination for '%s': %s\n", *restoreAlias, destPath)
		}
		return writeCfg()

	default:
		printUsageTo(stdout)
		return ExitInvalidArgs
	}
}

func printUsageTo(w io.Writer) {
	fmt.Fprintln(w, "mdsource - Manage source directories for medorg backup and journal")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  mdsource add -path <directory> -alias <shortcode>")
	fmt.Fprintln(w, "  mdsource remove -alias <shortcode>")
	fmt.Fprintln(w, "  mdsource list")
	fmt.Fprintln(w, "  mdsource restore -alias <shortcode> [-path <destination>]")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Examples:")
	fmt.Fprintln(w, "  mdsource add -path /mnt/hda1/media -alias media")
	fmt.Fprintln(w, "  mdsource remove -alias media")
	fmt.Fprintln(w, "  mdsource list")
	fmt.Fprintln(w, "  mdsource restore -alias media -path /new/media/location")
	fmt.Fprintln(w, "  mdsource restore -alias media  # uses source path as default")
}
