package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cbehopkins/medorg/pkg/core"
)

const (
	ExitOk = iota
	ExitSuppliedDirNotFound
	ExitWalkError
	ExitJournalWriteError
	ExitNoConfig
	ExitNoSourcesConfigured
)

func isDir(fn string) bool {
	stat, err := os.Stat(fn)
	if os.IsNotExist(err) {
		return false
	}
	if os.IsExist(err) || err == nil {
		if stat.IsDir() {
			return true
		}
	}
	return false
}

func main() {
	var directories []string
	var xc *core.XMLCfg
	scanflg := flag.Bool("scan", false, "Only scan files in src & dst updating labels, don't run the backup")

	flag.Parse()

	// Load XMLCfg
	var err error
	if xmcf := core.XmConfig(); xmcf != "" {
		xc, err = core.NewXMLCfg(string(xmcf))
		if err != nil {
			fmt.Println("Error loading config file:", err)
			os.Exit(ExitNoConfig)
		}
	} else {
		fn := filepath.Join(string(core.HomeDir()), "/.core.xml")
		xc, err = core.NewXMLCfg(fn)
		if err != nil {
			fmt.Println("Error creating config file:", err)
			os.Exit(ExitNoConfig)
		}
	}

	if xc == nil {
		fmt.Println("Unable to get config")
		os.Exit(ExitNoConfig)
	}

	// Get directories: command line args take precedence, otherwise use config
	if flag.NArg() > 0 {
		for _, fl := range flag.Args() {
			_, err := os.Stat(fl)
			if os.IsNotExist(err) {
				fmt.Println(fl, "does not exist!")
				os.Exit(ExitSuppliedDirNotFound)
			}
			if isDir(fl) {
				directories = append(directories, fl)
			}
		}
	} else {
		// Use source directories from config
		directories = xc.GetSourcePaths()
		if len(directories) == 0 {
			fmt.Println("No source directories configured. Use 'mdsource add' to configure sources or provide directories as arguments.")
			os.Exit(ExitNoSourcesConfigured)
		}
	}

	// Create alias lookup function if config available
	var getAlias func(string) string
	if xc != nil {
		getAlias = xc.GetAliasForPath
	}

	cfg := Config{
		Directories:  directories,
		JournalPath:  string(core.ConfigPath(".mdjournal.xml")),
		ScanOnly:     *scanflg,
		ReadExisting: true,
		GetAlias:     getAlias,
	}

	exitCode, err := Run(cfg)
	if err != nil {
		fmt.Println(err)
	}
	os.Exit(exitCode)
}
