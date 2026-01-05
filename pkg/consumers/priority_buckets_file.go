package consumers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
	"github.com/cbehopkins/bobbob/yggdrasil/vault"
)

// filePriorityBuckets persists buckets using a vault, one collection per bucket key.
// Bucket identities are types.IntKey(key). Payloads are stored keyed by size-desc/path-asc
// composite strings so iteration yields items in the desired order without extra sorting.
type filePriorityBuckets struct {
	session     *vault.VaultSession
	collections map[int]*treap.PersistentPayloadTreap[types.StringKey, fileData]
	tmpFile     string
}

func newFilePriorityBuckets() (*filePriorityBuckets, error) {
	tmpFile := filepath.Join(os.TempDir(), fmt.Sprintf("priority_buckets_%d.db", time.Now().UnixNano()))

	session, _, err := vault.OpenVaultWithIdentity[types.IntKey](tmpFile)
	if err != nil {
		return nil, err
	}
	// Keep memory bounded similarly to other components.
	session.Vault.SetMemoryBudgetWithPercentile(1000, 25)

	return &filePriorityBuckets{
		session:     session,
		collections: make(map[int]*treap.PersistentPayloadTreap[types.StringKey, fileData]),
		tmpFile:     tmpFile,
	}, nil
}

func (fpb *filePriorityBuckets) Close() error {
	if fpb.session != nil {
		_ = fpb.session.Close()
	}
	if fpb.tmpFile != "" {
		_ = os.Remove(fpb.tmpFile)
	}
	return nil
}

func (fpb *filePriorityBuckets) getOrCreateCollection(key int) (*treap.PersistentPayloadTreap[types.StringKey, fileData], error) {
	if coll, ok := fpb.collections[key]; ok {
		return coll, nil
	}

	coll, err := vault.GetOrCreateCollectionWithIdentity(
		fpb.session.Vault,
		types.IntKey(key),
		types.StringLess,
		(*types.StringKey)(new(string)),
		fileData{},
	)
	if err != nil {
		return nil, err
	}
	fpb.collections[key] = coll
	return coll, nil
}

func (fpb *filePriorityBuckets) add(key int, fd fileData) error {
	coll, err := fpb.getOrCreateCollection(key)
	if err != nil {
		return err
	}
	k := buildFileKey(fd)
	coll.Insert(&k, fd)
	return nil
}

func (fpb *filePriorityBuckets) iterate() func() ([]fileData, bool) {
	keys := make([]int, 0, len(fpb.collections))
	for k := range fpb.collections {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	idx := 0

	return func() ([]fileData, bool) {
		for idx < len(keys) {
			coll := fpb.collections[keys[idx]]
			idx++
			bucket, err := streamCollectionBucket(coll)
			if err != nil {
				return nil, false
			}
			if len(bucket) == 0 {
				continue
			}
			return bucket, true
		}
		return nil, false
	}
}

// buildFileKey encodes size-descending and path-ascending ordering into the key
// so collection iteration yields the desired ordering without extra sorting.
func buildFileKey(fd fileData) types.StringKey {
	// Invert the size so smaller numeric keys correspond to larger files.
	keyNum := ^uint64(fd.Size)
	// FIXME we should use a custom key here that doesn't require stringing
	// We could just set the compare methos to only compare the
	return types.StringKey(fmt.Sprintf("%020d:%s", keyNum, fd.Fpath))
}

// streamCollectionBucket reads all items from a collection in order (size-desc, path-asc).
func streamCollectionBucket(coll *treap.PersistentPayloadTreap[types.StringKey, fileData]) ([]fileData, error) {
	bucket := make([]fileData, 0)
	ctx := context.Background()

	for node, err := range coll.Iter(ctx) {
		if err != nil {
			return nil, err
		}
		payloadNode, ok := node.(treap.PersistentPayloadNodeInterface[types.StringKey, fileData])
		if !ok {
			return nil, fmt.Errorf("unexpected payload node type")
		}
		bucket = append(bucket, payloadNode.GetPayload())
	}

	return bucket, nil
}
