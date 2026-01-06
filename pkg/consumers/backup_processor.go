package consumers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/vault"
	"github.com/cbehopkins/medorg/pkg/core"
)

type fileData struct {
	Size       int64
	Fpath      core.Fpath
	BackupDest []string
}

// Marshal implements PersistentPayload interface
func (f fileData) Marshal() ([]byte, error) {
	data := fmt.Sprintf("%d:%v", f.Size, f.BackupDest)
	return []byte(data), nil
}

// Unmarshal implements PersistentPayload interface
func (f fileData) Unmarshal(data []byte) (treap.UntypedPersistentPayload, error) {
	var size int64
	var backupDest []string
	_, err := fmt.Sscanf(string(data), "%d:%v", &size, &backupDest)
	if err != nil {
		return nil, err
	}
	f.Size = size
	f.BackupDest = backupDest
	return f, nil
}

// SizeInBytes implements PersistentPayload interface
func (f fileData) SizeInBytes() int {
	data, err := f.Marshal()
	if err != nil {
		return 0
	}
	return len(data)
}

type BackupProcessor struct {
	srcFileCollection *treap.PersistentPayloadTreap[treap.MD5Key, fileData]
	dstFileCollection *treap.PersistentPayloadTreap[treap.MD5Key, fileData]
	session           *vault.VaultSession
	filePath          string
}

// // md5KeyFromHexString parses a hex-encoded MD5 digest into a treap.MD5Key.
// func md5KeyFromHexString(md5Key string) (treap.MD5Key, error) {
// 	return treap.MD5KeyFromString(md5Key)
// }

// md5KeyFromBase64String parses a base64 (no padding) MD5 digest into a treap.MD5Key.
// func md5KeyFromBase64String(md5Key string) (treap.MD5Key, error) {
// }

// md5KeyFromMedorgString converts a medorg checksum to a treap.MD5Key.
// It tries hex first for forward compatibility, then base64 (current format).
func md5KeyFromMedorgString(md5Key string) (treap.MD5Key, error) {
	// if key, err := md5KeyFromHexString(md5Key); err == nil {
	// 	return key, nil
	// }

	return treap.Md5KeyFromBase64String(md5Key)
	// return md5KeyFromBase64String(md5Key)
}

func NewBackupProcessor() (*BackupProcessor, error) {
	tmpDir := os.TempDir()
	tmpFile := filepath.Join(tmpDir, fmt.Sprintf("backup_processor_%d.db", time.Now().UnixNano()))

	session, colls, err := vault.OpenVaultWithIdentity(
		tmpFile,
		vault.PayloadIdentitySpec[string, treap.MD5Key, fileData]{
			Identity:        "srcFiles",
			LessFunc:        treap.MD5Less,
			KeyTemplate:     (*treap.MD5Key)(new(treap.MD5Key)),
			PayloadTemplate: fileData{},
		},
		vault.PayloadIdentitySpec[string, treap.MD5Key, fileData]{
			Identity:        "dstFiles",
			LessFunc:        treap.MD5Less,
			KeyTemplate:     (*treap.MD5Key)(new(treap.MD5Key)),
			PayloadTemplate: fileData{},
		},
	)
	if err != nil {
		return nil, err
	}

	srcCollection, ok := colls["srcFiles"].(*treap.PersistentPayloadTreap[treap.MD5Key, fileData])
	if !ok {
		return nil, fmt.Errorf("collection has wrong type: got %T", colls["srcFiles"])
	}
	dstCollection, ok := colls["dstFiles"].(*treap.PersistentPayloadTreap[treap.MD5Key, fileData])
	if !ok {
		return nil, fmt.Errorf("collection has wrong type: got %T", colls["dstFiles"])
	}
	// Set memory budget: keep max 50 nodes, flush oldest 50% when exceeded
	// This is aggressive but necessary on Pi with limited RAM
	session.Vault.SetMemoryBudgetWithPercentile(50, 50)
	return &BackupProcessor{
		srcFileCollection: srcCollection,
		dstFileCollection: dstCollection,
		session:           session,
		filePath:          tmpFile,
	}, nil
}

func (bp *BackupProcessor) Close() error {
	defer os.Remove(bp.filePath)
	return bp.session.Close()
}

// Add files to the list of files we found
// A file is defined by it's md5 key - with a sanity check on its size
// backupDest is where it's already backed up to
// Then srcDir and srcFile are where we find it to back it up from
func (bp *BackupProcessor) addSrcFile(md5Key string, size int64, backupDest []string, file core.Fpath) error {
	// Try hex string first (for tests), then fall back to base64 (production format)
	key, err := treap.MD5KeyFromString(md5Key)
	if err != nil {
		// If hex fails, try base64
		key, err = treap.Md5KeyFromBase64String(md5Key)
		if err != nil {
			return fmt.Errorf("invalid md5 key %q: %w", md5Key, err)
		}
	}
	payload := fileData{
		Size:       size,
		Fpath:      file,
		BackupDest: backupDest,
	}
	bp.srcFileCollection.Insert(&key, payload)
	return nil
}

// Add files to the list of files we found in the backup destination
func (bp *BackupProcessor) addDstFile(md5Key string, size int64, backupDest []string, file core.Fpath) error {
	// Try hex string first (for tests), then fall back to base64 (production format)
	key, err := treap.MD5KeyFromString(md5Key)
	if err != nil {
		// If hex fails, try base64
		key, err = treap.Md5KeyFromBase64String(md5Key)
		if err != nil {
			return fmt.Errorf("invalid md5 key %q: %w", md5Key, err)
		}
	}
	payload := fileData{
		Size:       size,
		Fpath:      file,
		BackupDest: backupDest,
	}
	bp.dstFileCollection.Insert(&key, payload)
	return nil
}

// Return list of files to backup in a prioritized fashion
// Uses vault-based sorting to avoid in-memory sort operations
func (bp *BackupProcessor) prioritizedSrcFiles() (func() (core.Fpath, bool), error) {
	// Create a temporary collection for sorting by priority
	identity := fmt.Sprintf("priority_%d", time.Now().UnixNano())
	priorityColl, err := vault.GetOrCreateCollectionWithIdentity(
		bp.session.Vault,
		identity,
		priorityKeyLess,
		(*priorityKey)(new(priorityKey)),
		fileData{},
	)
	if err != nil {
		return nil, err
	}

	// Callback: insert files that are only in src (not in dst) into priority collection
	onlyInSrc := func(node treap.TreapNodeInterface[treap.MD5Key]) error {
		payloadNode, ok := node.(treap.PersistentPayloadNodeInterface[treap.MD5Key, fileData])
		if !ok {
			return fmt.Errorf("unexpected node type %T", node)
		}
		fd := payloadNode.GetPayload()
		key := buildPriorityKey(fd)
		priorityColl.Insert(&key, fd)
		return nil
	}

	// Ignore files in both collections
	inBoth := func(_ treap.TreapNodeInterface[treap.MD5Key], _ treap.TreapNodeInterface[treap.MD5Key]) error {
		return nil
	}

	// Ignore files only in dst
	onlyInDst := func(_ treap.TreapNodeInterface[treap.MD5Key]) error {
		return nil
	}

	// Compare and populate priority collection with only src files
	if err := bp.srcFileCollection.Compare(bp.dstFileCollection, onlyInSrc, inBoth, onlyInDst); err != nil {
		return nil, err
	}

	// Iterate through the sorted collection and collect paths in order
	ordered := make([]core.Fpath, 0)
	ctx := context.Background()
	for node, iterErr := range priorityColl.Iter(ctx) {
		if iterErr != nil {
			return nil, iterErr
		}
		payloadNode, ok := node.(treap.PersistentPayloadNodeInterface[priorityKey, fileData])
		if !ok {
			return nil, fmt.Errorf("unexpected node type %T", node)
		}
		ordered = append(ordered, payloadNode.GetPayload().Fpath)
	}

	idx := 0
	next := func() (core.Fpath, bool) {
		if idx >= len(ordered) {
			return core.Fpath(""), false
		}
		fp := ordered[idx]
		idx++
		return fp, true
	}

	return next, nil
}

type fileDataWithMd5 struct {
	fileData
	MD5Key string
}
type inMemoryBackupProcessor struct {
	srcFiles      map[string]*fileData
	dstFiles      map[string]*fileData
	filesByLength [][]*fileDataWithMd5
}

func NewInMemoryBackupProcessor() *inMemoryBackupProcessor {
	return &inMemoryBackupProcessor{
		srcFiles: make(map[string]*fileData),
		dstFiles: make(map[string]*fileData),
	}
}

// Add files to the list of files we found
// A file is defined by it's md5 key - with a sanity check on its size
// backupDest is where it's already backed up to
// Then srcDir and srcFile are where we find it to back it up from
func (bp *inMemoryBackupProcessor) addSrcFile(md5Key string, size int64, backupDest []string, file core.Fpath) error {
	bp.srcFiles[md5Key] = &fileData{
		Size:       size,
		Fpath:      file,
		BackupDest: backupDest,
	}
	return nil
}

func (bp *inMemoryBackupProcessor) addDstFile(md5Key string, size int64, backupDest []string, file core.Fpath) error {
	bp.dstFiles[md5Key] = &fileData{
		Size:       size,
		Fpath:      file,
		BackupDest: backupDest,
	}
	return nil
}

func (bp *inMemoryBackupProcessor) Close() error {
	// No resources to clean up in this in-memory implementation
	return nil
}

// Return list of files to backup in a prioritized fashion
// We sort by: fewest destinations first, then largest size first
func (bp *inMemoryBackupProcessor) prioritizedSrcFiles() (func() (core.Fpath, bool), error) {
	// Collect files present in src but not in dst.
	entries := make([]fileData, 0, len(bp.srcFiles))
	for md5Key, src := range bp.srcFiles {
		if _, exists := bp.dstFiles[md5Key]; exists {
			continue
		}
		entries = append(entries, *src)
	}

	// Sort by: fewest destinations first, then largest size first, deterministic fallback on path.
	sort.Slice(entries, func(i, j int) bool {
		lenI, lenJ := len(entries[i].BackupDest), len(entries[j].BackupDest)
		if lenI != lenJ {
			return lenI < lenJ
		}
		if entries[i].Size != entries[j].Size {
			return entries[i].Size > entries[j].Size
		}
		return entries[i].Fpath < entries[j].Fpath
	})

	idx := 0
	next := func() (core.Fpath, bool) {
		if idx >= len(entries) {
			return core.Fpath(""), false
		}
		fp := entries[idx].Fpath
		idx++
		return fp, true
	}

	return next, nil
}
