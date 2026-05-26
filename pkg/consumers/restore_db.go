package consumers

import (
	"fmt"
	"sync"

	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/vault"
)

// RestoreDB manages the restore task collections (pending and copied).
type RestoreDB struct {
	session        *vault.VaultSession
	pendingColl    *treap.PersistentPayloadTreap[restoreContentKey, RestoreTaskNode]
	copiedColl     *treap.PersistentPayloadTreap[restoreContentKey, RestoreTaskNode]
	filePath       string
	mu             sync.Mutex
	opsWg          sync.WaitGroup // Tracks active operations
	done           chan struct{}   // Closed to signal shutdown
	closeOnce      sync.Once       // Ensures Close is called only once
	persistQueue   chan persistRequest
	persistWg      sync.WaitGroup
}

// OpenRestoreDB opens or creates a restore DB at the given path.
func OpenRestoreDB(filePath string) (*RestoreDB, error) {
	session, colls, err := vault.OpenVaultWithIdentity(
		filePath,
		vault.PayloadIdentitySpec[string, restoreContentKey, RestoreTaskNode]{
			Identity:        RestoreCollectionPending,
			LessFunc:        restoreContentKeyLess,
			KeyTemplate:     (*restoreContentKey)(new(restoreContentKey)),
			PayloadTemplate: RestoreTaskNode{},
		},
		vault.PayloadIdentitySpec[string, restoreContentKey, RestoreTaskNode]{
			Identity:        RestoreCollectionCopied,
			LessFunc:        restoreContentKeyLess,
			KeyTemplate:     (*restoreContentKey)(new(restoreContentKey)),
			PayloadTemplate: RestoreTaskNode{},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to open vault: %w", err)
	}

	pendingColl, ok := colls[RestoreCollectionPending].(*treap.PersistentPayloadTreap[restoreContentKey, RestoreTaskNode])
	if !ok {
		return nil, fmt.Errorf("pending collection has wrong type: got %T", colls[RestoreCollectionPending])
	}

	copiedColl, ok := colls[RestoreCollectionCopied].(*treap.PersistentPayloadTreap[restoreContentKey, RestoreTaskNode])
	if !ok {
		return nil, fmt.Errorf("copied collection has wrong type: got %T", colls[RestoreCollectionCopied])
	}

	// Disable background monitoring (we'll use explicit persist)
	session.Vault.SetBackgroundMonitoring(false)

	db := &RestoreDB{
		session:      session,
		pendingColl:  pendingColl,
		copiedColl:   copiedColl,
		filePath:     filePath,
		done:         make(chan struct{}),
		persistQueue: make(chan persistRequest, 100),
	}

	db.persistWg.Add(1)
	go db.persistWorker()

	return db, nil
}

func (db *RestoreDB) persistWorker() {
	defer db.persistWg.Done()
	for req := range db.persistQueue {
		req.errCh <- req.fn()
	}
}

func (db *RestoreDB) beginOp() (func(), error) {
	db.opsWg.Add(1)
	select {
	case <-db.done:
		db.opsWg.Done()
		return nil, fmt.Errorf("RestoreDB is shutting down")
	default:
		return db.opsWg.Done, nil
	}
}

// InsertPending adds a target to the pending collection, creating or appending to the content node.
func (db *RestoreDB) InsertPending(target *RestoreTaskTarget, md5 string, size int64, backupDests []string) error {
	finisher, err := db.beginOp()
	if err != nil {
		return err
	}
	defer finisher()

	db.mu.Lock()
	defer db.mu.Unlock()

	key := NewRestoreContentKey(md5, size)

	// Check if this content already exists
	node := db.pendingColl.Search(&key)
	if node != nil {
		// Append to existing node
		existing := node.GetPayload()
		existing = existing.WithTarget(*target, backupDests)
		return db.pendingColl.UpdatePayload(&key, existing)
	}

	// Create new node
	newNode := NewRestoreTaskNode(*target, md5, size, backupDests)
	return db.pendingColl.Insert(&key, newNode)
}

// InsertCopied adds a target to the copied collection.
func (db *RestoreDB) InsertCopied(target *RestoreTaskTarget, md5 string, size int64, backupDests []string) error {
	finisher, err := db.beginOp()
	if err != nil {
		return err
	}
	defer finisher()

	db.mu.Lock()
	defer db.mu.Unlock()

	key := NewRestoreContentKey(md5, size)

	// Check if this content already exists in copied
	node := db.copiedColl.Search(&key)
	if node != nil {
		// Append to existing node
		existing := node.GetPayload()
		existing = existing.WithTarget(*target, backupDests)
		return db.copiedColl.UpdatePayload(&key, existing)
	}

	// Create new node
	newNode := NewRestoreTaskNode(*target, md5, size, backupDests)
	return db.copiedColl.Insert(&key, newNode)
}

// FindPendingByContent searches the pending collection for targets with matching (md5, size).
// Uses efficient Search instead of full scan.
func (db *RestoreDB) FindPendingByContent(md5 string, size int64) ([]RestoreTaskTarget, error) {
	finisher, err := db.beginOp()
	if err != nil {
		return nil, err
	}
	defer finisher()

	db.mu.Lock()
	defer db.mu.Unlock()

	key := NewRestoreContentKey(md5, size)
	node := db.pendingColl.Search(&key)
	if node == nil {
		return []RestoreTaskTarget{}, nil
	}

	payloadNode, ok := node.(treap.PersistentPayloadNodeInterface[restoreContentKey, RestoreTaskNode])
	if !ok {
		return nil, fmt.Errorf("unexpected node type %T", node)
	}

	return payloadNode.GetPayload().Targets, nil
}

// MoveToCopied moves a specific target from pending to copied collection.
// Removes target from pending node, appends to copied node.
func (db *RestoreDB) MoveToCopied(targetPath string, md5 string, size int64) error {
	finisher, err := db.beginOp()
	if err != nil {
		return err
	}
	defer finisher()

	db.mu.Lock()
	defer db.mu.Unlock()

	key := NewRestoreContentKey(md5, size)

	// Get pending node
	pendingNode := db.pendingColl.Search(&key)
	if pendingNode == nil {
		return fmt.Errorf("content not found in pending: %s, %d", md5, size)
	}

	payloadNode, ok := pendingNode.(treap.PersistentPayloadNodeInterface[restoreContentKey, RestoreTaskNode])
	if !ok {
		return fmt.Errorf("unexpected node type %T", pendingNode)
	}

	pending := payloadNode.GetPayload()

	// Find and remove target from pending
	updatedPending, removedTarget, ok := pending.RemoveTarget(targetPath)
	if !ok {
		return fmt.Errorf("target %s not found in pending", targetPath)
	}

	// If pending is now empty, delete the node; otherwise update it
	if len(updatedPending.Targets) == 0 {
		if err := db.pendingColl.Delete(&key); err != nil {
			return fmt.Errorf("failed to delete empty pending node: %w", err)
		}
	} else {
		if err := db.pendingColl.UpdatePayload(&key, updatedPending); err != nil {
			return fmt.Errorf("failed to update pending: %w", err)
		}
	}

	// Add to copied
	copiedNode := db.copiedColl.Search(&key)
	if copiedNode != nil {
		// Append to existing copied node
		payloadNode, ok := copiedNode.(treap.PersistentPayloadNodeInterface[restoreContentKey, RestoreTaskNode])
		if !ok {
			return fmt.Errorf("unexpected copied node type %T", copiedNode)
		}
		existing := payloadNode.GetPayload()
		existing = existing.WithTarget(removedTarget, nil)
		return db.copiedColl.UpdatePayload(&key, existing)
	}

	// Create new copied node
	newCopied := NewRestoreTaskNode(removedTarget, md5, size, pending.BackupDests)
	return db.copiedColl.Insert(&key, newCopied)
}

// CountPending returns the number of tasks in the pending collection.
func (db *RestoreDB) CountPending() (int, error) {
	finisher, err := db.beginOp()
	if err != nil {
		return 0, err
	}
	defer finisher()

	db.mu.Lock()
	defer db.mu.Unlock()

	count, err := db.pendingColl.Count()
	return count, err
}

// CountCopied returns the number of tasks in the copied collection.
func (db *RestoreDB) CountCopied() (int, error) {
	finisher, err := db.beginOp()
	if err != nil {
		return 0, err
	}
	defer finisher()

	db.mu.Lock()
	defer db.mu.Unlock()

	count, err := db.copiedColl.Count()
	return count, err
}

// CountPendingByBackupDest returns a map of backup destination to count of pending targets.
// This allows reporting which volumes still have pending files to restore.
func (db *RestoreDB) CountPendingByBackupDest() (map[string]int, error) {
	finisher, err := db.beginOp()
	if err != nil {
		return nil, err
	}
	defer finisher()

	db.mu.Lock()
	defer db.mu.Unlock()

	result := make(map[string]int)

	// Iterate through all pending content nodes
	err = db.pendingColl.InOrderVisit(func(node treap.TreapNodeInterface[restoreContentKey]) error {
		payloadNode, ok := node.(treap.PersistentPayloadNodeInterface[restoreContentKey, RestoreTaskNode])
		if !ok {
			return fmt.Errorf("unexpected node type %T", node)
		}
		
		payload := payloadNode.GetPayload()
		// Each target needs this content from one of the backup destinations
		for _, dest := range payload.BackupDests {
			result[dest] += len(payload.Targets)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

// Close closes the restore DB and releases resources.
func (db *RestoreDB) Close() error {
	var closeErr error
	db.closeOnce.Do(func() {
		close(db.done)
		db.opsWg.Wait()
		close(db.persistQueue)
		db.persistWg.Wait()
		closeErr = db.session.Close()
	})
	return closeErr
}

// WithRestoreDB opens a restore DB, runs fn, and always closes the DB.
// If both fn and close return errors, both are preserved in the returned error.
func WithRestoreDB(filePath string, fn func(*RestoreDB) error) (err error) {
	db, err := OpenRestoreDB(filePath)
	if err != nil {
		return err
	}

	defer func() {
		closeErr := db.Close()
		if closeErr == nil {
			return
		}
		if err == nil {
			err = closeErr
			return
		}
		err = fmt.Errorf("%w; close restore db: %w", err, closeErr)
	}()

	err = fn(db)
	return err
}
