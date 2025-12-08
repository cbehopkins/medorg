package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/cbehopkins/medorg/pkg/adaptive"
	"github.com/cbehopkins/medorg/pkg/cli"
	"github.com/cbehopkins/medorg/pkg/consumers"
	"github.com/cbehopkins/medorg/pkg/core"
)

func main() {
	var directories []string
	var xc *core.MdConfig

	configPath := flag.String("config", "", "Path to config file (optional, defaults to ~/.medorg.xml)")
	scrubflg := flag.Bool("scrub", false, "Scrub all backup labels from src records")
	calcCnt := flag.Int("calc", 2, "Max Number of MD5 calculators")
	delflg := flag.Bool("delete", false, "Delete duplicated Files")
	mvdflg := flag.Bool("mvd", false, "Move Detect")
	rnmflg := flag.Bool("rename", false, "Auto Rename Files")
	rclflg := flag.Bool("recalc", false, "Recalculate all checksums")
	valflg := flag.Bool("validate", false, "Validate all checksums")
	adaptiveflg := flag.Bool("adaptive", false, "Enable adaptive token tuning to find optimal concurrency")

	flag.Parse()

	// Load XMLCfg (needed for rename and for getting source directories)
	var err error
	if *configPath != "" || core.XmConfig() != "" {
		xc, err = core.LoadOrCreateMdConfigWithPath(*configPath)
		if err != nil {
			fmt.Println("Error loading config file:", err)
			// Don't exit - config is optional for basic operations
			xc = nil
		}
	} else if *rnmflg {
		// Only error if rename was requested but no config found
		fmt.Println("no config file found (required for rename)")
		xc, err = core.LoadOrCreateMdConfig()
		if err != nil {
			fmt.Println("Error creating config file:", err)
			os.Exit(5)
		}
	}

	// Get directories: command line args take precedence, otherwise use config
	resolver := cli.NewSourceDirResolver(flag.Args(), xc, os.Stdout)
	directories, _ = resolver.Resolve()
	if len(directories) == 0 {
		directories = []string{"."}
	}

	// Setup AutoFix if rename flag is set
	var AF *consumers.AutoFix
	if *rnmflg {
		if xc == nil {
			fmt.Println("Error: config file required for rename operation")
			os.Exit(5)
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

	// Create adaptive tuner if requested
	var tuner *adaptive.Tuner
	if *adaptiveflg {
		tuner = adaptive.NewTunerWithConfig(1, 2*runtime.NumCPU(), 60*time.Second)
		// When using tuner, set CalcCount to max tokens so we have enough workers
		*calcCnt = 2 * runtime.NumCPU()
		fmt.Printf("Adaptive tuning enabled (workers: %d, tokens: 1-%d, check interval: 60s)\n",
			*calcCnt, 2*runtime.NumCPU())
	}

	// Run the check_calc operation using the extracted package function
	opts := consumers.CheckCalcOptions{
		CalcCount: *calcCnt,
		Recalc:    *rclflg,
		Validate:  *valflg,
		Scrub:     *scrubflg,
		AutoFix:   AF,
		Tuner:     tuner,
	}

	err = consumers.RunCheckCalc(directories, opts)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(2)
	}

	fmt.Println("Finished walking")
}
