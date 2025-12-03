package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cbehopkins/medorg/pkg/consumers"
	"github.com/cbehopkins/medorg/pkg/core"
)

func isDir(fn string) bool {
	stat, err := os.Stat(fn)
	if err != nil {
		return false
	}
	return stat.IsDir()
}

func main() {
	var directories []string

	scrubflg := flag.Bool("scrub", false, "Scrub all backup labels from src records")
	calcCnt := flag.Int("calc", 2, "Max Number of MD5 calculators")
	delflg := flag.Bool("delete", false, "Delete duplicated Files")
	mvdflg := flag.Bool("mvd", false, "Move Detect")
	rnmflg := flag.Bool("rename", false, "Auto Rename Files")
	rclflg := flag.Bool("recalc", false, "Recalculate all checksums")
	valflg := flag.Bool("validate", false, "Validate all checksums")

	flag.Parse()
	if flag.NArg() > 0 {
		for _, fl := range flag.Args() {
			if isDir(fl) {
				directories = append(directories, fl)
			}
		}
	} else {
		directories = []string{"."}
	}

	// Setup AutoFix if rename flag is set
	var AF *consumers.AutoFix
	if *rnmflg {
		var xc *core.XMLCfg
		var err error
		if xmcf := core.XmConfig(); xmcf != "" {
			xc, err = core.NewXMLCfg(string(xmcf))
			if err != nil {
				fmt.Println("Error loading config file:", err)
				os.Exit(5)
			}
		} else {
			fmt.Println("no config file found")
			fn := filepath.Join(string(core.HomeDir()), core.Md5FileName)
			xc, err = core.NewXMLCfg(fn)
			if err != nil {
				fmt.Println("Error creating config file:", err)
				os.Exit(5)
			}
		}
		AF = consumers.NewAutoFix(xc.Af)
		AF.DeleteFiles = *delflg
	}

	// Handle move detection separately
	if *mvdflg {
		err := consumers.RunMoveDetect(directories)
		if err != nil {
			fmt.Println("Error! In move detect", err)
			os.Exit(4)
		}
		fmt.Println("Finished move detection")
	}

	// Run the check_calc operation using the extracted package function
	opts := consumers.CheckCalcOptions{
		CalcCount: *calcCnt,
		Recalc:    *rclflg,
		Validate:  *valflg,
		Scrub:     *scrubflg,
		AutoFix:   AF,
	}

	err := consumers.RunCheckCalc(directories, opts)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(2)
	}

	fmt.Println("Finished walking")
}
