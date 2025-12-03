package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
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
)

func main() {
	retcode := 0
	defer func() { os.Exit(retcode) }()

	// Command line argument processing
	addCmd := flag.NewFlagSet("add", flag.ExitOnError)
	addPath := addCmd.String("path", "", "Directory path to add")
	addAlias := addCmd.String("alias", "", "Alias/shortcode for the directory")

	removeCmd := flag.NewFlagSet("remove", flag.ExitOnError)
	removeAlias := removeCmd.String("alias", "", "Alias to remove")

	listCmd := flag.NewFlagSet("list", flag.ExitOnError)

	if len(os.Args) < 2 {
		printUsage()
		retcode = ExitInvalidArgs
		return
	}

	// Load or create XMLCfg
	var xc *core.XMLCfg
	var err error

	if xmcf := core.XmConfig(); xmcf != "" {
		xc, err = core.NewXMLCfg(string(xmcf))
		if err != nil {
			fmt.Println("Error loading config file:", err)
			retcode = ExitNoConfig
			return
		}
	} else {
		fn := filepath.Join(string(core.HomeDir()), "/.core.xml")
		xc, err = core.NewXMLCfg(fn)
		if err != nil {
			fmt.Println("Error creating config file:", err)
			retcode = ExitNoConfig
			return
		}
	}

	if xc == nil {
		fmt.Println("Unable to get config")
		retcode = ExitNoConfig
		return
	}

	defer func() {
		if retcode == ExitOk {
			err := xc.WriteXmlCfg()
			if err != nil {
				fmt.Println("Error while saving config file:", err)
				retcode = ExitNoConfig
			}
		}
	}()

	// Parse subcommand
	switch os.Args[1] {
	case "add":
		if err := addCmd.Parse(os.Args[2:]); err != nil {
			fmt.Println("Error parsing add command:", err)
			retcode = ExitInvalidArgs
			return
		}

		if *addPath == "" || *addAlias == "" {
			fmt.Println("Error: both -path and -alias are required")
			addCmd.PrintDefaults()
			retcode = ExitInvalidArgs
			return
		}

		// Verify path exists
		if _, err := os.Stat(*addPath); os.IsNotExist(err) {
			fmt.Printf("Error: path '%s' does not exist\n", *addPath)
			retcode = ExitPathNotExist
			return
		}

		// Add to config
		if !xc.AddSourceDirectory(*addPath, *addAlias) {
			fmt.Printf("Error: alias '%s' already exists\n", *addAlias)
			retcode = ExitAliasExists
			return
		}

		fmt.Printf("Added source directory: %s -> %s\n", *addAlias, *addPath)

	case "remove":
		if err := removeCmd.Parse(os.Args[2:]); err != nil {
			fmt.Println("Error parsing remove command:", err)
			retcode = ExitInvalidArgs
			return
		}

		if *removeAlias == "" {
			fmt.Println("Error: -alias is required")
			removeCmd.PrintDefaults()
			retcode = ExitInvalidArgs
			return
		}

		if !xc.RemoveSourceDirectory(*removeAlias) {
			fmt.Printf("Error: alias '%s' not found\n", *removeAlias)
			retcode = ExitAliasNotFound
			return
		}

		fmt.Printf("Removed source directory with alias: %s\n", *removeAlias)

	case "list":
		if err := listCmd.Parse(os.Args[2:]); err != nil {
			fmt.Println("Error parsing list command:", err)
			retcode = ExitInvalidArgs
			return
		}

		if len(xc.SourceDirectories) == 0 {
			fmt.Println("No source directories configured")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ALIAS\tPATH")
		fmt.Fprintln(w, "-----\t----")
		for _, sd := range xc.SourceDirectories {
			fmt.Fprintf(w, "%s\t%s\n", sd.Alias, sd.Path)
		}
		w.Flush()

	default:
		printUsage()
		retcode = ExitInvalidArgs
	}
}

func printUsage() {
	fmt.Println("mdsource - Manage source directories for medorg backup and journal")
	fmt.Println("")
	fmt.Println("Usage:")
	fmt.Println("  mdsource add -path <directory> -alias <shortcode>")
	fmt.Println("  mdsource remove -alias <shortcode>")
	fmt.Println("  mdsource list")
	fmt.Println("")
	fmt.Println("Examples:")
	fmt.Println("  mdsource add -path /mnt/hda1/media -alias media")
	fmt.Println("  mdsource remove -alias media")
	fmt.Println("  mdsource list")
}
