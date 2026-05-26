package consumers

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

type priorityQueueJSONLEntry struct {
	Timestamp          int64    `json:"timestamp"`
	Path               string   `json:"path"`
	Size               int64    `json:"size"`
	BackupDestinations []string `json:"backup_destinations"`
}

// createPriorityQueueJSONL writes deterministic, production-like JSONL data to a test temp dir.
func createPriorityQueueJSONL(t *testing.T, count int) string {
	t.Helper()

	if count <= 0 {
		t.Fatalf("count must be positive, got %d", count)
	}

	jsonlPath := filepath.Join(t.TempDir(), "backup_priority_queue.jsonl")
	f, err := os.Create(jsonlPath)
	if err != nil {
		t.Fatalf("failed to create fixture JSONL %s: %v", jsonlPath, err)
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 1<<20)
	enc := json.NewEncoder(w)

	for i := 0; i < count; i++ {
		dests := make([]string, 0, i%4)
		for j := 0; j < i%4; j++ {
			dests = append(dests, fmt.Sprintf("VOL_%02d", j+1))
		}

		entry := priorityQueueJSONLEntry{
			Timestamp:          1700000000 + int64(i),
			Path:               fmt.Sprintf("/synthetic/source/%02d/file_%06d.dat", i%128, i),
			Size:               int64(1024 + (i % 100000)),
			BackupDestinations: dests,
		}

		if err := enc.Encode(entry); err != nil {
			t.Fatalf("failed to encode JSONL entry %d: %v", i, err)
		}
	}

	if err := w.Flush(); err != nil {
		t.Fatalf("failed to flush JSONL fixture: %v", err)
	}

	return jsonlPath
}
