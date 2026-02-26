//go:build debugpprof

package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	rpprof "runtime/pprof"
	"time"
)

var (
	flagPprofHTTPAddr        *string
	flagProfileOutDir        *string
	flagHeapProfileEvery     *time.Duration
	flagMemThresholdMB       *int
	flagMemProfileRate       *int
	flagBlockProfileRate     *int
	flagMutexProfileFraction *int
	flagCPUProfilePath       *string
	flagDumpOnInterrupt      *bool
)

func pprofAddFlags() {
	flagPprofHTTPAddr = flag.String("pprof-http", "", "Start net/http/pprof server at address (e.g. localhost:6060); empty disables")
	flagProfileOutDir = flag.String("profile-out", "pprof", "Directory to write profiles")
	flagHeapProfileEvery = flag.Duration("heap-profile-every", 0, "Interval to write periodic heap profiles (0 disables)")
	flagMemThresholdMB = flag.Int("mem-threshold-mb", 0, "Dump profiles when Alloc exceeds this many MB (0 disables)")
	flagMemProfileRate = flag.Int("mem-profile-rate", 0, "Set runtime.MemProfileRate bytes per sample (0 keeps default)")
	flagBlockProfileRate = flag.Int("block-profile-rate", 0, "Set runtime block profile rate; >0 enables profiling")
	flagMutexProfileFraction = flag.Int("mutex-profile-fraction", 0, "Set runtime mutex profile fraction; >0 enables profiling")
	flagCPUProfilePath = flag.String("cpu-profile", "", "Write CPU profile to this file for the duration of the run")
	flagDumpOnInterrupt = flag.Bool("dump-profiles-on-interrupt", true, "Write heap/goroutine profiles on first Ctrl-C")
}

func ensureDir(dir string) error {
	if dir == "" {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}

func startPprofHTTP(addr string) {
	if addr == "" {
		return
	}
	go func() {
		log.Printf("[PPROF] HTTP server listening on %s", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Printf("[PPROF] HTTP server error on %s: %v", addr, err)
		}
	}()
}

func writeProfile(kind, outPath string, debug int) error {
	pf := rpprof.Lookup(kind)
	if pf == nil {
		return fmt.Errorf("profile %s not available", kind)
	}
	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := pf.WriteTo(f, debug); err != nil {
		return err
	}
	log.Printf("[PPROF] wrote %s profile: %s", kind, outPath)
	return nil
}

func dumpAllProfiles(prefix, outDir string) {
	if outDir == "" {
		outDir = "."
	}
	_ = ensureDir(outDir)
	ts := time.Now().Format("20060102-150405")
	base := fmt.Sprintf("%s-%s", prefix, ts)
	_ = writeProfile("heap", filepath.Join(outDir, base+"-heap.pprof"), 0)
	_ = writeProfile("goroutine", filepath.Join(outDir, base+"-goroutine.txt"), 2)
	_ = writeProfile("allocs", filepath.Join(outDir, base+"-allocs.pprof"), 0)
	_ = writeProfile("mutex", filepath.Join(outDir, base+"-mutex.pprof"), 0)
	_ = writeProfile("block", filepath.Join(outDir, base+"-block.pprof"), 0)
}

func startHeapProfiler(interval time.Duration, outDir string, done <-chan struct{}) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				dumpAllProfiles("periodic", outDir)
			case <-done:
				return
			}
		}
	}()
}

func startMemoryThresholdWatcher(thresholdMB int, outDir string, done <-chan struct{}) {
	if thresholdMB <= 0 {
		return
	}
	threshold := uint64(thresholdMB) * 1024 * 1024
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				if m.Alloc >= threshold {
					log.Printf("[PPROF] Alloc exceeded threshold %d MB (Alloc=%d MB)", thresholdMB, m.Alloc/1024/1024)
					dumpAllProfiles("threshold", outDir)
				}
			case <-done:
				return
			}
		}
	}()
}

func pprofInit(done <-chan struct{}, _ any) func() {
	if *flagMemProfileRate > 0 {
		runtime.MemProfileRate = *flagMemProfileRate
		log.Printf("[PPROF] MemProfileRate set to %d", *flagMemProfileRate)
	}
	if *flagBlockProfileRate > 0 {
		runtime.SetBlockProfileRate(*flagBlockProfileRate)
		log.Printf("[PPROF] BlockProfileRate set to %d", *flagBlockProfileRate)
	}
	if *flagMutexProfileFraction > 0 {
		runtime.SetMutexProfileFraction(*flagMutexProfileFraction)
		log.Printf("[PPROF] MutexProfileFraction set to %d", *flagMutexProfileFraction)
	}
	if *flagCPUProfilePath != "" {
		if err := ensureDir(filepath.Dir(*flagCPUProfilePath)); err == nil {
			if fcpu, err := os.Create(*flagCPUProfilePath); err == nil {
				if err := rpprof.StartCPUProfile(fcpu); err == nil {
					log.Printf("[PPROF] CPU profiling to %s", *flagCPUProfilePath)
					// stop when process exits (deferred in caller is not possible here), rely on SIGINT or natural exit
					// since this is best-effort, we attach a goroutine to stop when done channel closes
					go func() {
						<-done
						rpprof.StopCPUProfile()
						_ = fcpu.Close()
						log.Printf("[PPROF] CPU profiling stopped")
					}()
				} else {
					log.Printf("[PPROF] failed to start CPU profile: %v", err)
				}
			} else {
				log.Printf("[PPROF] failed to create CPU profile file: %v", err)
			}
		}
	}
	_ = ensureDir(*flagProfileOutDir)
	startPprofHTTP(*flagPprofHTTPAddr)
	startHeapProfiler(*flagHeapProfileEvery, *flagProfileOutDir, done)
	startMemoryThresholdWatcher(*flagMemThresholdMB, *flagProfileOutDir, done)

	return func() {
		if flagDumpOnInterrupt != nil && *flagDumpOnInterrupt {
			dumpAllProfiles("interrupt", *flagProfileOutDir)
		}
	}
}
