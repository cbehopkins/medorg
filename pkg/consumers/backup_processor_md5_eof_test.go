package consumers

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/vault"
	"github.com/cbehopkins/medorg/pkg/core"
)

// TestMD5KeyUnexpectedEOF reproduces unexpected EOF errors when reading MD5 key objects
// from the treap store under heavy flush pressure.
func TestMD5KeyUnexpectedEOF(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running EOF reproduction test in short mode")
	}

	tmpDir := t.TempDir()
	tmpFile := tmpDir + "/md5_key_eof.db"

	itemCount := getEnvInt("MEDORG_EOF_ITEM_COUNT", 100_000)
	persistEvery := getEnvInt("MEDORG_EOF_PERSIST_EVERY", 0)
	matchEvery := getEnvInt("MEDORG_EOF_MATCH_EVERY", 1000)

	session, colls, err := vault.OpenVaultWithIdentity(
		tmpFile,
		vault.PayloadIdentitySpec[string, md5Key, fileData]{
			Identity:        "srcFiles",
			LessFunc:        md5KeyLess,
			KeyTemplate:     (*md5Key)(new(md5Key)),
			PayloadTemplate: fileData{},
		},
	)
	if err != nil {
		t.Fatalf("OpenVaultWithIdentity failed: %v", err)
	}

	coll, ok := colls["srcFiles"].(*treap.PersistentPayloadTreap[md5Key, fileData])
	if !ok {
		_ = session.Close()
		t.Fatalf("collection has wrong type: got %T", colls["srcFiles"])
	}

	// NOTE: Do NOT call StartBackgroundMonitoring() - it creates contention with insertions
	// that leads to deadlock under heavy memory pressure. Instead, rely on explicit Persist() calls.
	// SetMemoryBudgetWithPercentile is also disabled to avoid cascading flushes that interfere with insertions.

	start := time.Now()
	for i := 0; i < itemCount; i++ {
		key := md5Key{}
		binary.LittleEndian.PutUint64(key[0:8], uint64(i))
		binary.LittleEndian.PutUint64(key[8:16], uint64(i)^0xdeadbeef)
		payload := fileData{
			Size:       int64(i + 1),
			Fpath:      core.NewFpath(fmt.Sprintf("/file_%d.bin", i)),
			BackupDest: []string{"dest1"},
		}
		coll.Insert(&key, payload)

		// Simulate occasional MD5 key matches (read + update) to exercise lazy-load paths.
		if matchEvery > 0 && i > 0 && i%matchEvery == 0 {
			matchKey := md5Key{}
			binary.LittleEndian.PutUint64(matchKey[0:8], uint64(i-matchEvery))
			binary.LittleEndian.PutUint64(matchKey[8:16], uint64(i-matchEvery)^0xdeadbeef)
			if node := coll.Search(&matchKey); node != nil && !node.IsNil() {
				p := node.GetPayload()
				p.BackupDest = append(p.BackupDest, "dest2")
				if err := coll.UpdatePayload(&matchKey, p); err != nil {
					_ = session.Close()
					t.Fatalf("UpdatePayload failed at %d: %v", i, err)
				}
			}
		}

		if persistEvery > 0 && (i+1)%persistEvery == 0 {
			if err := coll.Persist(); err != nil {
				_ = session.Close()
				t.Fatalf("Persist failed at %d: %v", i+1, err)
			}
			t.Logf("Inserted %d/%d", i+1, itemCount)
		}
	}

	if err := coll.Persist(); err != nil {
		_ = session.Close()
		t.Fatalf("Final Persist failed: %v", err)
	}

	if err := session.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and iterate to trigger lazy-load from disk
	session2, colls2, err := vault.OpenVaultWithIdentity(
		tmpFile,
		vault.PayloadIdentitySpec[string, md5Key, fileData]{
			Identity:        "srcFiles",
			LessFunc:        md5KeyLess,
			KeyTemplate:     (*md5Key)(new(md5Key)),
			PayloadTemplate: fileData{},
		},
	)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer session2.Close()

	coll2, ok := colls2["srcFiles"].(*treap.PersistentPayloadTreap[md5Key, fileData])
	if !ok {
		t.Fatalf("reopened collection has wrong type: got %T", colls2["srcFiles"])
	}

	count := 0
	iterErr := coll2.InOrderVisit(func(node treap.TreapNodeInterface[md5Key]) error {
		if node == nil || node.IsNil() {
			return nil
		}
		count++
		return nil
	})

	if iterErr != nil {
		t.Fatalf("InOrderVisit failed after %d items (elapsed %s): %v", count, time.Since(start), iterErr)
	}

	if count != itemCount {
		t.Fatalf("Expected %d items, got %d", itemCount, count)
	}
}

func getEnvInt(name string, fallback int) int {
	val := os.Getenv(name)
	if val == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return fallback
	}
	return parsed
}
