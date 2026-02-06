package consumers

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
	"github.com/cbehopkins/bobbob/yggdrasil/vault"
	"github.com/cbehopkins/medorg/pkg/core"
)

type fileData struct {
	Size       int64
	Fpath      core.Fpath
	BackupDest []string
}

// mergeBackupDestinations unions two backup destination lists, preserving order of first appearance.
func mergeBackupDestinations(a, b []string) []string {
	seen := make(map[string]bool, len(a)+len(b))
	res := make([]string, 0, len(a)+len(b))
	for _, v := range a {
		if !seen[v] {
			seen[v] = true
			res = append(res, v)
		}
	}
	for _, v := range b {
		if !seen[v] {
			seen[v] = true
			res = append(res, v)
		}
	}
	return res
}

// Marshal implements PersistentPayload interface
func (f fileData) Marshal() ([]byte, error) {
	// Create a serializable version with just the path string (not the Fpath struct with cache pointers)
	type fileDataSerialized struct {
		Size       int64
		FpathStr   string
		BackupDest []string
	}
	serialized := fileDataSerialized{
		Size:       f.Size,
		FpathStr:   f.Fpath.String(),
		BackupDest: f.BackupDest,
	}
	jsonData, err := json.Marshal(serialized)
	if err != nil {
		return nil, err
	}
	// Length-prefixed format to handle fixed-size block allocations
	length := uint32(len(jsonData))
	buf := make([]byte, 4+len(jsonData))
	binary.LittleEndian.PutUint32(buf[0:4], length)
	copy(buf[4:], jsonData)
	return buf, nil
}

// Unmarshal implements PersistentPayload interface
func (f fileData) Unmarshal(data []byte) (types.UntypedPersistentPayload, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("fileData too short: %d bytes", len(data))
	}
	length := binary.LittleEndian.Uint32(data[0:4])
	if int(length) > len(data)-4 {
		return nil, fmt.Errorf("fileData length %d exceeds data size %d", length, len(data)-4)
	}
	type fileDataSerialized struct {
		Size       int64
		FpathStr   string
		BackupDest []string
	}
	var serialized fileDataSerialized
	err := json.Unmarshal(data[4:4+length], &serialized)
	if err != nil {
		return nil, err
	}
	return fileData{
		Size:       serialized.Size,
		Fpath:      core.NewFpath(serialized.FpathStr),
		BackupDest: serialized.BackupDest,
	}, nil
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
	srcFileCollection *treap.PersistentPayloadTreap[types.MD5Key, fileData]
	dstFileCollection *treap.PersistentPayloadTreap[types.MD5Key, fileData]
	session           *vault.VaultSession
	filePath          string
	mu                sync.Mutex          // Serializes treap/vault writes
	persistQueue      chan persistRequest // Serializes persist operations to prevent allocator corruption
	persistWg         sync.WaitGroup      // Tracks persist operations
	done              chan struct{}       // Closed to signal shutdown
	closeOnce         sync.Once           // Ensures Close is called only once
}

// persistRequest represents a request to persist a treap node
type persistRequest struct {
	fn    func() error // Function to call to perform the persist
	errCh chan error
}

func NewBackupProcessor() (*BackupProcessor, error) {
	tmpDir := os.TempDir()
	// Use CreateTemp to avoid filename collisions under parallel tests
	f, err := os.CreateTemp(tmpDir, "backup_processor_*.db")
	if err != nil {
		return nil, err
	}
	tmpFile := f.Name()
	// We only need the path for the vault; close the file handle
	_ = f.Close()

	session, colls, err := vault.OpenVaultWithIdentity(
		tmpFile,
		vault.PayloadIdentitySpec[string, types.MD5Key, fileData]{
			Identity:        "srcFiles",
			LessFunc:        types.MD5Less,
			KeyTemplate:     (*types.MD5Key)(new(types.MD5Key)),
			PayloadTemplate: fileData{},
		},
		vault.PayloadIdentitySpec[string, types.MD5Key, fileData]{
			Identity:        "dstFiles",
			LessFunc:        types.MD5Less,
			KeyTemplate:     (*types.MD5Key)(new(types.MD5Key)),
			PayloadTemplate: fileData{},
		},
	)
	if err != nil {
		return nil, err
	}

	srcCollection, ok := colls["srcFiles"].(*treap.PersistentPayloadTreap[types.MD5Key, fileData])
	if !ok {
		return nil, fmt.Errorf("collection has wrong type: got %T", colls["srcFiles"])
	}
	dstCollection, ok := colls["dstFiles"].(*treap.PersistentPayloadTreap[types.MD5Key, fileData])
	if !ok {
		return nil, fmt.Errorf("collection has wrong type: got %T", colls["dstFiles"])
	}
	// Set memory budget: keep max 50 nodes, flush oldest 50% when exceeded
	// This is aggressive but necessary on Pi with limited RAM
	// shouldFlushDebug := func(stats vault.MemoryStats, sf bool) {
	// 	if sf {
	// 		log.Println("[MEMORY] Flushing memory: ", stats)
	// 	} else {
	// 		log.Println("[MEMORY] Memory usage within budget", stats)
	// 	}

	// }
	// onFlushDebug := func(stats vault.MemoryStats, cnt int) {
	// 	log.Printf("[MEMORY] Flushed %d nodes, current stats: %v", cnt, stats)
	// }
	session.Vault.SetMemoryBudgetWithPercentile(10_000, 25)
	session.Vault.SetCheckInterval(1000)

	// Setup allocation logging for debugging memory issues
	// setupAllocationLogging(session)

	bp := &BackupProcessor{
		srcFileCollection: srcCollection,
		dstFileCollection: dstCollection,
		session:           session,
		filePath:          tmpFile,
		persistQueue:      make(chan persistRequest, 100), // Buffered queue for persist requests
		done:              make(chan struct{}),            // Closed to signal shutdown
	}

	// Start background persist worker to serialize persist operations
	bp.persistWg.Add(1)
	go bp.persistWorker()

	return bp, nil
}

func (bp *BackupProcessor) persistWorker() {
	defer bp.persistWg.Done()
	for req := range bp.persistQueue {
		// Serialize persist operations to prevent allocator corruption
		req.errCh <- req.fn()
	}
}

// queuePersist queues a persist operation and waits for it to complete
func (bp *BackupProcessor) queuePersist(fn func() error) error {
	errCh := make(chan error, 1)
	req := persistRequest{
		fn:    fn,
		errCh: errCh,
	}

	// Use select to avoid sending on closed channel
	select {
	case bp.persistQueue <- req:
		return <-errCh
	case <-bp.done:
		return fmt.Errorf("BackupProcessor is shutting down")
	}
}

func (bp *BackupProcessor) Close() error {
	var closeErr error
	bp.closeOnce.Do(func() {
		close(bp.done)         // Signal shutdown to all goroutines
		close(bp.persistQueue) // Close queue so persistWorker exits
		bp.persistWg.Wait()    // Wait for persistWorker to finish
		defer os.Remove(bp.filePath)
		closeErr = bp.session.Close()
	})
	return closeErr
}

// setupAllocationLogging configures allocation logging on the store's allocators for debugging memory usage.
// Uses type assertion to access SetOnAllocate callbacks on allocator interfaces.
func setupAllocationLogging(session *vault.VaultSession) {
	if session == nil || session.Vault == nil || session.Vault.Store == nil {
		return
	}
	ok := session.ConfigureAllocatorCallbacks(
		func(objId bobbob.ObjectId, offset bobbob.FileOffset, size int) {
			if size > 4096 {
				log.Printf("[ALLOC] WARNING: Large allocation %d bytes at offset %d (objId=%d)",
					size, offset, objId)
			}
		},
		func(objId bobbob.ObjectId, offset bobbob.FileOffset, size int) {
			log.Printf("[ALLOC] Parent allocator allocated %d bytes at offset %d (objId=%d)",
				size, offset, objId)
		},
	)
	if !ok {
		panic("Allocator shenannigans")
	}
}

// Add files to the list of files we found
// A file is defined by it's md5 key - with a sanity check on its size
// backupDest is where it's already backed up to
// Then srcDir and srcFile are where we find it to back it up from
func (bp *BackupProcessor) addSrcFile(md5Key string, size int64, backupDest []string, file core.Fpath) error {
	// Check if shutting down
	select {
	case <-bp.done:
		return fmt.Errorf("BackupProcessor is shutting down")
	default:
	}

	bp.mu.Lock()
	defer bp.mu.Unlock()
	// Try hex string first (for tests), then fall back to base64 (production format)
	key, err := types.MD5KeyFromString(md5Key)
	if err != nil {
		// If hex fails, try base64
		key, err = types.Md5KeyFromBase64String(md5Key)
		if err != nil {
			return fmt.Errorf("invalid md5 key %q: %w", md5Key, err)
		}
	}
	payload := fileData{
		Size:       size,
		Fpath:      file,
		BackupDest: backupDest,
	}

	// If we already have this checksum recorded, merge destinations and prefer the path
	// that needs backup most (fewest existing destinations) while retaining merged metadata.
	if existingNode := bp.srcFileCollection.Search(&key); existingNode != nil {
		existing := existingNode.GetPayload()
		mergedDest := mergeBackupDestinations(existing.BackupDest, payload.BackupDest)
		chosen := existing
		if len(payload.BackupDest) < len(existing.BackupDest) {
			chosen.Fpath = payload.Fpath
			chosen.Size = payload.Size
		}
		chosen.BackupDest = mergedDest
		existingNode.SetPayload(chosen)
		return bp.queuePersist(existingNode.Persist)
	}

	bp.srcFileCollection.Insert(&key, payload)
	return nil
}

// Add files to the list of files we found in the backup destination
func (bp *BackupProcessor) addDstFile(md5Key string, size int64, backupDest []string, file core.Fpath) error {
	// Check if shutting down
	select {
	case <-bp.done:
		return fmt.Errorf("BackupProcessor is shutting down")
	default:
	}

	bp.mu.Lock()
	defer bp.mu.Unlock()
	// Try hex string first (for tests), then fall back to base64 (production format)
	key, err := types.MD5KeyFromString(md5Key)
	if err != nil {
		// If hex fails, try base64
		key, err = types.Md5KeyFromBase64String(md5Key)
		if err != nil {
			return fmt.Errorf("invalid md5 key %q: %w", md5Key, err)
		}
	}
	payload := fileData{
		Size:       size,
		Fpath:      file,
		BackupDest: backupDest,
	}

	// Merge duplicate destination entries for the same checksum, tracking all destinations
	if existingNode := bp.dstFileCollection.Search(&key); existingNode != nil {
		existing := existingNode.GetPayload()
		// FIXME this feels over complex
		// Why isn't merging them sufficient?
		mergedDest := mergeBackupDestinations(existing.BackupDest, payload.BackupDest)
		chosen := existing
		// Keep the first seen path; destinations are what matter here
		chosen.BackupDest = mergedDest
		// Why can't we just set the node and let the memory management handle it?
		existingNode.SetPayload(chosen)
		return bp.queuePersist(existingNode.Persist)
	}

	bp.dstFileCollection.Insert(&key, payload)
	return nil
}

// checkDstFileExists checks if a file with the given checksum exists in destination
// Returns the file path and true if found, empty string and false otherwise
func (bp *BackupProcessor) checkDstFileExists(checksum string) (core.Fpath, bool) {
	key, err := types.MD5KeyFromString(checksum)
	if err != nil {
		// Try base64 format
		key, err = types.Md5KeyFromBase64String(checksum)
		if err != nil {
			return core.Fpath{}, false
		}
	}

	node := bp.dstFileCollection.Search(&key)
	if node == nil || node.IsNil() {
		return core.Fpath{}, false
	}
	return node.GetPayload().Fpath, true
}

// getOrphanFiles streams files in destination that weren't matched in any source.
// These are candidates for cleanup/archival. Results are yielded via iterator to
// avoid loading all orphans into memory at once.
func (bp *BackupProcessor) getOrphanFiles() (func(yield func(core.Fpath) bool), error) {
	iterator := func(yield func(core.Fpath) bool) {
		// Callback: ignore files only in src (not relevant for orphans)
		onlyInSrc := func(_ treap.TreapNodeInterface[types.MD5Key]) error { return nil }

		// Callback: ignore files in both (they're not orphans)
		inBoth := func(_ treap.TreapNodeInterface[types.MD5Key], _ treap.TreapNodeInterface[types.MD5Key]) error {
			return nil
		}

		// Callback: collect files only in dst (these are orphans)
		onlyInDst := func(node treap.TreapNodeInterface[types.MD5Key]) error {
			payloadNode, ok := node.(treap.PersistentPayloadNodeInterface[types.MD5Key, fileData])
			if !ok {
				return fmt.Errorf("unexpected node type %T", node)
			}
			if !yield(payloadNode.GetPayload().Fpath) {
				return fmt.Errorf("iterator stopped")
			}
			return nil
		}

		_ = bp.srcFileCollection.Compare(bp.dstFileCollection, onlyInSrc, inBoth, onlyInDst)
	}

	return iterator, nil
}

// Return list of files to backup in a prioritized fashion
// Uses vault-based sorting to avoid in-memory sort operations
// Yields fileData to provide access to size and backup destinations
func (bp *BackupProcessor) prioritizedSrcFiles() (func(yield func(fileData) bool), error) {
	// Create a temporary collection for sorting by priority
	// identity is just the name of the collection...
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

	addedToPriority := 0
	// Callback: insert files that are only in src (not in dst) into priority collection
	onlyInSrc := func(node treap.TreapNodeInterface[types.MD5Key]) error {
		payloadNode, ok := node.(treap.PersistentPayloadNodeInterface[types.MD5Key, fileData])
		if !ok {
			return fmt.Errorf("unexpected node type %T", node)
		}
		fd := payloadNode.GetPayload()
		key := buildPriorityKey(fd)
		priorityColl.Insert(&key, fd)
		addedToPriority++
		log.Println("[Compare] Added file to priority collection", fd.Fpath.String())
		return nil
	}

	// Ignore files in both collections
	inBoth := func(node treap.TreapNodeInterface[types.MD5Key], _ treap.TreapNodeInterface[types.MD5Key]) error {
		payloadNode, ok := node.(treap.PersistentPayloadNodeInterface[types.MD5Key, fileData])
		if !ok {
			return fmt.Errorf("unexpected node type %T", node)
		}
		fd := payloadNode.GetPayload()

		log.Println("[Compare] Skipping file in both src and dst", fd.Fpath.String())
		return nil
	}

	// Ignore files only in dst
	onlyInDst := func(node treap.TreapNodeInterface[types.MD5Key]) error {
		payloadNode, ok := node.(treap.PersistentPayloadNodeInterface[types.MD5Key, fileData])
		if !ok {
			return fmt.Errorf("unexpected node type %T", node)
		}
		fd := payloadNode.GetPayload()

		log.Println("[Compare] Skipping file only in dst", fd.Fpath.String())
		return nil
	}

	// Compare and populate priority collection with only src files
	if err := bp.srcFileCollection.Compare(bp.dstFileCollection, onlyInSrc, inBoth, onlyInDst); err != nil {
		return nil, err
	}
	log.Printf("[Compare] Added %d files to priority collection", addedToPriority)

	// Return an iterator that walks the persistent treap in-order, including
	// nodes that have been flushed to disk.
	iterator := func(yield func(fileData) bool) {
		yieldedCount := 0
		stopErr := errors.New("iteration stopped by caller")

		err := priorityColl.InOrderVisit(func(node treap.TreapNodeInterface[priorityKey]) error {
			payloadNode, ok := node.(treap.PersistentPayloadNodeInterface[priorityKey, fileData])
			if !ok {
				return fmt.Errorf("unexpected node type %T", node)
			}
			payload := payloadNode.GetPayload()
			yieldedCount++

			if yieldedCount <= 20 || yieldedCount%50000 == 0 {
				log.Printf("[Iterator] Yielding file %d: %s (size: %d, dests: %v)\n",
					yieldedCount, payload.Fpath.String(), payload.Size, payload.BackupDest)
			}

			if !yield(payload) {
				log.Println("[Iterator] Stopping iteration as requested by caller")
				return stopErr
			}
			return nil
		})
		if err != nil && !errors.Is(err, stopErr) {
			log.Printf("[Iterator] ERROR during InOrderVisit: %v\n", err)
		}
		log.Printf("[Iterator] Completed iterating prioritized source files: %d files yielded, %d files added\n",
			yieldedCount, addedToPriority)
	}

	return iterator, nil
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
	if existing, ok := bp.srcFiles[md5Key]; ok {
		merged := mergeBackupDestinations(existing.BackupDest, backupDest)
		if len(backupDest) < len(existing.BackupDest) {
			existing.Fpath = file
			existing.Size = size
		}
		existing.BackupDest = merged
		return nil
	}
	bp.srcFiles[md5Key] = &fileData{Size: size, Fpath: file, BackupDest: backupDest}
	return nil
}

func (bp *inMemoryBackupProcessor) addDstFile(md5Key string, size int64, backupDest []string, file core.Fpath) error {
	if existing, ok := bp.dstFiles[md5Key]; ok {
		existing.BackupDest = mergeBackupDestinations(existing.BackupDest, backupDest)
		return nil
	}
	bp.dstFiles[md5Key] = &fileData{Size: size, Fpath: file, BackupDest: backupDest}
	return nil
}

func (bp *inMemoryBackupProcessor) Close() error {
	// No resources to clean up in this in-memory implementation
	return nil
}

// Return list of files to backup in a prioritized fashion
// We sort by: fewest destinations first, then largest size first
// Yields fileData to provide access to size and backup destinations
func (bp *inMemoryBackupProcessor) prioritizedSrcFiles() (func(yield func(fileData) bool), error) {
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
		return entries[i].Fpath.String() < entries[j].Fpath.String()
	})

	iterator := func(yield func(fileData) bool) {
		for _, entry := range entries {
			if !yield(entry) {
				return
			}
		}
	}

	return iterator, nil
}
