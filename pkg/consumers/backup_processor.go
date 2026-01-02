package consumers

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
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

// md5KeyFromHexString parses a hex-encoded MD5 digest into a treap.MD5Key.
func md5KeyFromHexString(md5Key string) (treap.MD5Key, error) {
	return treap.MD5KeyFromString(md5Key)
}

// md5KeyFromBase64String parses a base64 (no padding) MD5 digest into a treap.MD5Key.
func md5KeyFromBase64String(md5Key string) (treap.MD5Key, error) {
	decoded, err := base64.StdEncoding.WithPadding(base64.NoPadding).DecodeString(md5Key)
	if err != nil {
		return treap.MD5Key{}, fmt.Errorf("invalid base64 md5 key %q: %w", md5Key, err)
	}
	if len(decoded) != md5.Size {
		return treap.MD5Key{}, fmt.Errorf("invalid base64 md5 key %q: expected %d bytes, got %d", md5Key, md5.Size, len(decoded))
	}
	return treap.MD5KeyFromString(hex.EncodeToString(decoded))
}

// md5KeyFromMedorgString converts a medorg checksum to a treap.MD5Key.
// It tries hex first for forward compatibility, then base64 (current format).
func md5KeyFromMedorgString(md5Key string) (treap.MD5Key, error) {
	if key, err := md5KeyFromHexString(md5Key); err == nil {
		return key, nil
	}

	return md5KeyFromBase64String(md5Key)
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
	// Set memory budget: keep max 1000 nodes, flush oldest 25% when exceeded
	// This means when we hit 1000 nodes, we'll flush 250 of the oldest ones
	session.Vault.SetMemoryBudgetWithPercentile(1000, 25)
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
	key, err := md5KeyFromMedorgString(md5Key)
	if err != nil {
		return err
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
	key, err := md5KeyFromMedorgString(md5Key)
	if err != nil {
		return err
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
func (bp *BackupProcessor) prioritizedSrcFiles() (func() (core.Fpath, bool), error) {
	// Bucket files by how many destinations they are already backed up to, then within each
	// bucket sort by descending size so larger files are attempted first.
	// FIXME this should be in a temporary treap structure for efficiency with large datasets
	buckets := make(map[int][]fileData)

	onlyInSrc := func(node treap.TreapNodeInterface[treap.MD5Key]) error {
		payloadNode, ok := node.(treap.PersistentPayloadNodeInterface[treap.MD5Key, fileData])
		if !ok {
			return fmt.Errorf("unexpected node type %T", node)
		}
		fd := payloadNode.GetPayload()
		length := len(fd.BackupDest)
		buckets[length] = append(buckets[length], fd)
		return nil
	}

	// Differences only: files present in both or only in dst are ignored.
	inBoth := func(_ treap.TreapNodeInterface[treap.MD5Key], _ treap.TreapNodeInterface[treap.MD5Key]) error {
		return nil
	}
	onlyInDst := func(_ treap.TreapNodeInterface[treap.MD5Key]) error { return nil }

	if err := bp.srcFileCollection.Compare(bp.dstFileCollection, onlyInSrc, inBoth, onlyInDst); err != nil {
		return nil, err
	}

	// Order buckets by ascending BackupDest length.
	lengths := make([]int, 0, len(buckets))
	for l := range buckets {
		lengths = append(lengths, l)
	}
	sort.Ints(lengths)

	// Flatten into a single ordered list: smallest BackupDest count first, within each bucket largest size first.
	ordered := make([]core.Fpath, 0)
	for _, length := range lengths {
		bucket := buckets[length]
		sort.Slice(bucket, func(i, j int) bool {
			if bucket[i].Size == bucket[j].Size {
				return bucket[i].Fpath < bucket[j].Fpath
			}
			return bucket[i].Size > bucket[j].Size
		})
		for _, fd := range bucket {
			ordered = append(ordered, fd.Fpath)
		}
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

// FIXME add testing for this
func (bp *inMemoryBackupProcessor) addLengthedFile(md5Key string, fileData *fileData) {
	fd := &fileDataWithMd5{
		fileData: *fileData,
		MD5Key:   md5Key,
	}
	file_length := len(fileData.BackupDest)
	if (file_length + 1) > len(bp.filesByLength) {
		// Extend the slice to accommodate the new length
		newSlice := make([][]*fileDataWithMd5, file_length+1)
		copy(newSlice, bp.filesByLength)
		bp.filesByLength = newSlice
	}
	if bp.filesByLength[file_length] == nil {
		bp.filesByLength[file_length] = []*fileDataWithMd5{}
	}
	bp.filesByLength[file_length] = append(bp.filesByLength[file_length], fd)
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
