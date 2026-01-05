package consumers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
	"github.com/cbehopkins/bobbob/yggdrasil/vault"
	"github.com/cbehopkins/medorg/pkg/core"
)

type priorityBucketsInterface interface {
	add(key int, fd fileData) error
	iterate() func() ([]fileData, bool)
}

type bucketCleanup interface {
	Close() error
}

func getPriorityBuckets(t *testing.T) []struct {
	name    string
	pb      priorityBucketsInterface
	cleanup func()
} {
	fpb, err := newFilePriorityBuckets()
	if err != nil {
		t.Fatalf("failed to create filePriorityBuckets: %v", err)
	}

	return []struct {
		name    string
		pb      priorityBucketsInterface
		cleanup func()
	}{
		{name: "memory", pb: newPriorityBuckets(), cleanup: func() {}},
		{name: "file", pb: fpb, cleanup: func() { _ = fpb.Close() }},
	}
}

func TestPriorityBucketsIterateOrdersBucketsAndContents(t *testing.T) {
	for _, impl := range getPriorityBuckets(t) {
		impl := impl
		t.Run(impl.name, func(t *testing.T) {
			defer impl.cleanup()
			pb := impl.pb
			// bucket key 2
			if err := pb.add(2, fileData{Size: 100, Fpath: core.Fpath("/b")}); err != nil {
				t.Fatalf("add failed: %v", err)
			}
			if err := pb.add(2, fileData{Size: 300, Fpath: core.Fpath("/c")}); err != nil {
				t.Fatalf("add failed: %v", err)
			}
			// bucket key 1
			if err := pb.add(1, fileData{Size: 50, Fpath: core.Fpath("/x")}); err != nil {
				t.Fatalf("add failed: %v", err)
			}
			if err := pb.add(1, fileData{Size: 200, Fpath: core.Fpath("/a")}); err != nil {
				t.Fatalf("add failed: %v", err)
			}

			next := pb.iterate()

			b1, ok := next()
			if !ok {
				t.Fatalf("expected first bucket")
			}
			expectedB1 := []core.Fpath{core.Fpath("/a"), core.Fpath("/x")}
			if len(b1) != len(expectedB1) {
				t.Fatalf("first bucket len mismatch: got %d want %d", len(b1), len(expectedB1))
			}
			for i, fd := range b1 {
				if fd.Fpath != expectedB1[i] {
					t.Fatalf("first bucket pos %d: got %s want %s", i, fd.Fpath, expectedB1[i])
				}
			}

			b2, ok := next()
			if !ok {
				t.Fatalf("expected second bucket")
			}
			expectedB2 := []core.Fpath{core.Fpath("/c"), core.Fpath("/b")}
			if len(b2) != len(expectedB2) {
				t.Fatalf("second bucket len mismatch: got %d want %d", len(b2), len(expectedB2))
			}
			for i, fd := range b2 {
				if fd.Fpath != expectedB2[i] {
					t.Fatalf("second bucket pos %d: got %s want %s", i, fd.Fpath, expectedB2[i])
				}
			}

			if _, ok := next(); ok {
				t.Fatalf("expected no more buckets")
			}
		})
	}
}

// TestFilePriorityBucketsTreapIterationFaulty demonstrates the issue where treap.Iter(ctx)
// returns 0 items even after inserting data via Insert().
// This test uses only stdlib and bobbob APIs, without depending on filePriorityBuckets.
func TestFilePriorityBucketsTreapIterationFaulty(t *testing.T) {
	// Create a temporary vault session directly
	tmpFile := filepath.Join(os.TempDir(), fmt.Sprintf("treap_test_%d.db", time.Now().UnixNano()))
	defer os.Remove(tmpFile)

	session, _, err := vault.OpenVaultWithIdentity[types.IntKey](tmpFile)
	if err != nil {
		t.Fatalf("failed to open vault: %v", err)
	}
	defer session.Close()

	// Create a collection directly using vault APIs
	coll, err := vault.GetOrCreateCollectionWithIdentity(
		session.Vault,
		types.IntKey(1),
		types.StringLess,
		(*types.StringKey)(new(string)),
		fileData{},
	)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}

	// Insert two items directly into the treap
	key1 := types.StringKey("item1")
	fd1 := fileData{Size: 100, Fpath: core.Fpath("/file1")}
	coll.Insert(&key1, fd1)

	key2 := types.StringKey("item2")
	fd2 := fileData{Size: 200, Fpath: core.Fpath("/file2")}
	coll.Insert(&key2, fd2)

	t.Logf("✓ Inserted 2 items into treap collection")

	// Try to iterate the treap collection
	ctx := context.Background()
	treapItemCount := 0
	for node, err := range coll.Iter(ctx) {
		if err != nil {
			t.Fatalf("treap iteration error: %v", err)
		}
		payloadNode, ok := node.(treap.PersistentPayloadNodeInterface[types.StringKey, fileData])
		if !ok {
			t.Fatalf("unexpected node type: %T", node)
		}
		_ = payloadNode.GetPayload()
		treapItemCount++
	}

	// This demonstrates the bug: treapItemCount is 0 even though we inserted 2 items
	t.Logf("✗ Treap collection iteration returned %d items (expected 2)", treapItemCount)
	if treapItemCount != 0 {
		t.Logf("BUG FIXED: Treap iteration now returns items!")
	} else {
		t.Logf("BUG CONFIRMED: Treap iteration returns 0 items despite Insert calls succeeding")
	}
}
