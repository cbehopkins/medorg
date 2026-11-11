package core

import (
	"fmt"
	"io"
	"runtime"
)

// LogMemoryStats writes a snapshot of Go memory and goroutine usage.
func LogMemoryStats(writer io.Writer, prefix string) {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	fmt.Fprintf(writer, "%s Memory: Alloc=%d HeapInuse=%d Goroutines=%d\n",
		prefix, stats.Alloc, stats.HeapInuse, runtime.NumGoroutine())
}
