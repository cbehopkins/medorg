package consumers

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
	"github.com/cbehopkins/medorg/pkg/core"
	_ "modernc.org/sqlite"
)

const backupQueryBatchSize = 128

var errBackupShutdown = errors.New("BackupProcessor is shutting down")

type fileData struct {
	Size       int64
	Fpath      core.Fpath
	BackupDest []string
}

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

func (f fileData) Marshal() ([]byte, error) {
	type serializedFileData struct {
		Size       int64
		FpathStr   string
		BackupDest []string
	}
	data, err := json.Marshal(serializedFileData{
		Size:       f.Size,
		FpathStr:   f.Fpath.String(),
		BackupDest: f.BackupDest,
	})
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 4+len(data))
	binary.LittleEndian.PutUint32(buf, uint32(len(data)))
	copy(buf[4:], data)
	return buf, nil
}

func (f fileData) Unmarshal(data []byte) (types.UntypedPersistentPayload, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("fileData too short: %d bytes", len(data))
	}
	length := binary.LittleEndian.Uint32(data[:4])
	if uint64(length) > uint64(len(data)-4) {
		return nil, fmt.Errorf("fileData length %d exceeds data size %d", length, len(data)-4)
	}
	var serialized struct {
		Size       int64
		FpathStr   string
		BackupDest []string
	}
	if err := json.Unmarshal(data[4:4+int(length)], &serialized); err != nil {
		return nil, err
	}
	return fileData{
		Size:       serialized.Size,
		Fpath:      core.NewFpath(serialized.FpathStr),
		BackupDest: serialized.BackupDest,
	}, nil
}

func (f fileData) SizeInBytes() int {
	data, err := f.Marshal()
	if err != nil {
		return 0
	}
	return len(data)
}

type BackupProcessor struct {
	db        *sql.DB
	tempDir   string
	mu        sync.Mutex
	closed    bool
	closeOnce sync.Once
	closeErr  error
}

type priorityCursor struct {
	backupCount int
	size        int64
	path        string
	checksum    string
}

type orphanCursor struct {
	path     string
	checksum string
}

func NewBackupProcessor() (*BackupProcessor, error) {
	tempDir, err := os.MkdirTemp("", "medorg-backup-inventory-*")
	if err != nil {
		return nil, fmt.Errorf("create backup inventory directory: %w", err)
	}
	db, err := sql.Open("sqlite", filepath.Join(tempDir, "inventory.sqlite"))
	if err != nil {
		_ = os.RemoveAll(tempDir)
		return nil, fmt.Errorf("open backup inventory database: %w", err)
	}
	db.SetMaxOpenConns(1)
	bp := &BackupProcessor{db: db, tempDir: tempDir}
	if err := db.Ping(); err != nil {
		_ = bp.Close()
		return nil, fmt.Errorf("connect to backup inventory database: %w", err)
	}
	for _, statement := range []string{
		"PRAGMA temp_store = FILE",
		"PRAGMA cache_size = -2048",
		`CREATE TABLE source_inventory (
			checksum TEXT PRIMARY KEY,
			size INTEGER NOT NULL,
			backup_count INTEGER NOT NULL,
			backup_destinations TEXT NOT NULL,
			path TEXT NOT NULL
		)`,
		`CREATE TABLE destination_inventory (
			checksum TEXT PRIMARY KEY,
			size INTEGER NOT NULL,
			backup_count INTEGER NOT NULL,
			backup_destinations TEXT NOT NULL,
			path TEXT NOT NULL
		)`,
		`CREATE INDEX source_priority ON source_inventory (
			backup_count ASC,
			size DESC,
			path COLLATE BINARY ASC,
			checksum COLLATE BINARY ASC
		)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			_ = bp.Close()
			return nil, fmt.Errorf("initialise backup inventory database: %w", err)
		}
	}
	return bp, nil
}

func (bp *BackupProcessor) Close() error {
	bp.closeOnce.Do(func() {
		bp.mu.Lock()
		defer bp.mu.Unlock()
		bp.closed = true
		if err := bp.db.Close(); err != nil {
			bp.closeErr = errors.Join(bp.closeErr, fmt.Errorf("close backup inventory database: %w", err))
		}
		if err := os.RemoveAll(bp.tempDir); err != nil {
			bp.closeErr = errors.Join(bp.closeErr, fmt.Errorf("remove backup inventory directory: %w", err))
		}
	})
	return bp.closeErr
}

func (bp *BackupProcessor) ensureOpenLocked() error {
	if bp.closed {
		return errBackupShutdown
	}
	return nil
}

func (bp *BackupProcessor) addSrcFile(checksum string, size int64, backupDest []string, file core.Fpath) error {
	key, _, err := md5KeyFromString(checksum)
	if err != nil {
		return fmt.Errorf("invalid md5 key %q: %w", checksum, err)
	}
	checksum = fmt.Sprintf("%x", key[:])

	bp.mu.Lock()
	defer bp.mu.Unlock()
	if err := bp.ensureOpenLocked(); err != nil {
		return err
	}

	var existingPath, destinationsJSON string
	var existingSize int64
	err = bp.db.QueryRow(
		"SELECT size, backup_destinations, path FROM source_inventory WHERE checksum = ?",
		checksum,
	).Scan(&existingSize, &destinationsJSON, &existingPath)
	if errors.Is(err, sql.ErrNoRows) {
		return insertInventoryFile(bp.db, "source_inventory", checksum, size, backupDest, file)
	}
	if err != nil {
		return fmt.Errorf("read source inventory entry %q: %w", checksum, err)
	}

	var existingDestinations []string
	if err := json.Unmarshal([]byte(destinationsJSON), &existingDestinations); err != nil {
		return fmt.Errorf("decode source inventory destinations for %q: %w", checksum, err)
	}
	merged := mergeBackupDestinations(existingDestinations, backupDest)
	chosenPath, chosenSize := existingPath, existingSize
	if len(backupDest) < len(existingDestinations) {
		chosenPath, chosenSize = file.String(), size
	}
	mergedJSON, err := json.Marshal(merged)
	if err != nil {
		return fmt.Errorf("encode source inventory destinations for %q: %w", checksum, err)
	}
	_, err = bp.db.Exec(
		`UPDATE source_inventory
		 SET size = ?, backup_count = ?, backup_destinations = ?, path = ?
		 WHERE checksum = ?`,
		chosenSize, len(merged), string(mergedJSON), chosenPath, checksum,
	)
	if err != nil {
		return fmt.Errorf("update source inventory entry %q: %w", checksum, err)
	}
	return nil
}

func (bp *BackupProcessor) addDstFile(checksum string, size int64, backupDest []string, file core.Fpath) error {
	key, _, err := md5KeyFromString(checksum)
	if err != nil {
		return fmt.Errorf("invalid md5 key %q: %w", checksum, err)
	}
	checksum = fmt.Sprintf("%x", key[:])

	bp.mu.Lock()
	defer bp.mu.Unlock()
	if err := bp.ensureOpenLocked(); err != nil {
		return err
	}

	var destinationsJSON string
	err = bp.db.QueryRow(
		"SELECT backup_destinations FROM destination_inventory WHERE checksum = ?",
		checksum,
	).Scan(&destinationsJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return insertInventoryFile(bp.db, "destination_inventory", checksum, size, backupDest, file)
	}
	if err != nil {
		return fmt.Errorf("read destination inventory entry %q: %w", checksum, err)
	}

	var existingDestinations []string
	if err := json.Unmarshal([]byte(destinationsJSON), &existingDestinations); err != nil {
		return fmt.Errorf("decode destination inventory destinations for %q: %w", checksum, err)
	}
	mergedJSON, err := json.Marshal(mergeBackupDestinations(existingDestinations, backupDest))
	if err != nil {
		return fmt.Errorf("encode destination inventory destinations for %q: %w", checksum, err)
	}
	_, err = bp.db.Exec(
		`UPDATE destination_inventory
		 SET backup_count = ?, backup_destinations = ?
		 WHERE checksum = ?`,
		len(mergeBackupDestinations(existingDestinations, backupDest)), string(mergedJSON), checksum,
	)
	if err != nil {
		return fmt.Errorf("update destination inventory entry %q: %w", checksum, err)
	}
	return nil
}

func insertInventoryFile(db *sql.DB, table, checksum string, size int64, backupDest []string, file core.Fpath) error {
	destinationsJSON, err := json.Marshal(backupDest)
	if err != nil {
		return fmt.Errorf("encode backup destinations for %q: %w", checksum, err)
	}
	_, err = db.Exec(
		"INSERT INTO "+table+" (checksum, size, backup_count, backup_destinations, path) VALUES (?, ?, ?, ?, ?)",
		checksum, size, len(backupDest), string(destinationsJSON), file.String(),
	)
	if err != nil {
		return fmt.Errorf("insert inventory entry %q: %w", checksum, err)
	}
	return nil
}

func (bp *BackupProcessor) checkDstFileExists(checksum string) (core.Fpath, bool, error) {
	key, _, err := md5KeyFromString(checksum)
	if err != nil {
		return core.Fpath{}, false, fmt.Errorf("invalid md5 key %q: %w", checksum, err)
	}
	checksum = fmt.Sprintf("%x", key[:])

	bp.mu.Lock()
	defer bp.mu.Unlock()
	if err := bp.ensureOpenLocked(); err != nil {
		return core.Fpath{}, false, err
	}
	var path string
	err = bp.db.QueryRow("SELECT path FROM destination_inventory WHERE checksum = ?", checksum).Scan(&path)
	if errors.Is(err, sql.ErrNoRows) {
		return core.Fpath{}, false, nil
	}
	if err != nil {
		return core.Fpath{}, false, fmt.Errorf("check destination inventory for %q: %w", checksum, err)
	}
	return core.NewFpath(path), true, nil
}

func (bp *BackupProcessor) getOrphanFiles(shutdown <-chan struct{}) (func(yield func(core.Fpath) bool) error, error) {
	iterator := func(yield func(core.Fpath) bool) error {
		ctx, stop := backupQueryContext(shutdown)
		defer stop()
		var cursor orphanCursor
		hasCursor := false
		for {
			if err := checkBackupShutdown(shutdown); err != nil {
				return err
			}
			bp.mu.Lock()
			if err := bp.ensureOpenLocked(); err != nil {
				bp.mu.Unlock()
				return err
			}
			rows, err := bp.db.QueryContext(ctx, `
				SELECT d.path, d.checksum
				FROM destination_inventory d
				LEFT JOIN source_inventory s ON s.checksum = d.checksum
				WHERE s.checksum IS NULL
				  AND (? = 0 OR d.path COLLATE BINARY > ? OR (d.path COLLATE BINARY = ? AND d.checksum COLLATE BINARY > ?))
				ORDER BY d.path COLLATE BINARY ASC, d.checksum COLLATE BINARY ASC
				LIMIT ?`,
				boolInt(hasCursor), cursor.path, cursor.path, cursor.checksum, backupQueryBatchSize,
			)
			if err != nil {
				bp.mu.Unlock()
				if shutdownErr := checkBackupShutdown(shutdown); shutdownErr != nil {
					return shutdownErr
				}
				return fmt.Errorf("query orphaned destination inventory: %w", err)
			}
			paths := make([]core.Fpath, 0, backupQueryBatchSize)
			for rows.Next() {
				var path, checksum string
				if err := rows.Scan(&path, &checksum); err != nil {
					_ = rows.Close()
					bp.mu.Unlock()
					return fmt.Errorf("read orphaned destination inventory: %w", err)
				}
				cursor = orphanCursor{path: path, checksum: checksum}
				paths = append(paths, core.NewFpath(path))
			}
			queryErr := rows.Err()
			closeErr := rows.Close()
			bp.mu.Unlock()
			if err := errors.Join(queryErr, closeErr); err != nil {
				if shutdownErr := checkBackupShutdown(shutdown); shutdownErr != nil {
					return shutdownErr
				}
				return fmt.Errorf("iterate orphaned destination inventory: %w", err)
			}
			if len(paths) == 0 {
				return nil
			}
			hasCursor = true
			for _, path := range paths {
				if err := checkBackupShutdown(shutdown); err != nil {
					return err
				}
				if !yield(path) {
					return nil
				}
			}
		}
	}
	return iterator, nil
}

func (bp *BackupProcessor) prioritizedSrcFiles(maxNumBackups int, shutdown <-chan struct{}) (func(yield func(fileData) bool) error, error) {
	iterator := func(yield func(fileData) bool) error {
		ctx, stop := backupQueryContext(shutdown)
		defer stop()
		var cursor priorityCursor
		hasCursor := false
		for {
			if err := checkBackupShutdown(shutdown); err != nil {
				return err
			}
			bp.mu.Lock()
			if err := bp.ensureOpenLocked(); err != nil {
				bp.mu.Unlock()
				return err
			}
			rows, err := bp.db.QueryContext(ctx, `
				SELECT s.size, s.backup_destinations, s.path, s.backup_count, s.checksum
				FROM source_inventory s
				LEFT JOIN destination_inventory d ON d.checksum = s.checksum
				WHERE d.checksum IS NULL
				  AND s.backup_count <= ?
				  AND (
					? = 0
					OR s.backup_count > ?
					OR (s.backup_count = ? AND s.size < ?)
					OR (s.backup_count = ? AND s.size = ? AND s.path COLLATE BINARY > ?)
					OR (s.backup_count = ? AND s.size = ? AND s.path COLLATE BINARY = ? AND s.checksum COLLATE BINARY > ?)
				  )
				ORDER BY s.backup_count ASC, s.size DESC, s.path COLLATE BINARY ASC, s.checksum COLLATE BINARY ASC
				LIMIT ?`,
				maxNumBackups,
				boolInt(hasCursor),
				cursor.backupCount,
				cursor.backupCount, cursor.size,
				cursor.backupCount, cursor.size, cursor.path,
				cursor.backupCount, cursor.size, cursor.path, cursor.checksum,
				backupQueryBatchSize,
			)
			if err != nil {
				bp.mu.Unlock()
				if shutdownErr := checkBackupShutdown(shutdown); shutdownErr != nil {
					return shutdownErr
				}
				return fmt.Errorf("query prioritized source inventory: %w", err)
			}
			batch := make([]fileData, 0, backupQueryBatchSize)
			for rows.Next() {
				var fd fileData
				var destinationsJSON, path, checksum string
				var fdBackupCount int
				if err := rows.Scan(&fd.Size, &destinationsJSON, &path, &fdBackupCount, &checksum); err != nil {
					_ = rows.Close()
					bp.mu.Unlock()
					return fmt.Errorf("read prioritized source inventory: %w", err)
				}
				if err := json.Unmarshal([]byte(destinationsJSON), &fd.BackupDest); err != nil {
					_ = rows.Close()
					bp.mu.Unlock()
					return fmt.Errorf("decode prioritized source destinations for %q: %w", checksum, err)
				}
				fd.Fpath = core.NewFpath(path)
				batch = append(batch, fd)
				cursor = priorityCursor{backupCount: fdBackupCount, size: fd.Size, path: path, checksum: checksum}
			}
			queryErr := rows.Err()
			closeErr := rows.Close()
			bp.mu.Unlock()
			if err := errors.Join(queryErr, closeErr); err != nil {
				if shutdownErr := checkBackupShutdown(shutdown); shutdownErr != nil {
					return shutdownErr
				}
				return fmt.Errorf("iterate prioritized source inventory: %w", err)
			}
			if len(batch) == 0 {
				return nil
			}
			hasCursor = true
			for _, fd := range batch {
				if err := checkBackupShutdown(shutdown); err != nil {
					return err
				}
				if !yield(fd) {
					return nil
				}
			}
		}
	}
	return iterator, nil
}

func backupQueryContext(shutdown <-chan struct{}) (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	if shutdown == nil {
		return ctx, cancel
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-shutdown:
			cancel()
		case <-done:
		}
	}()
	return ctx, func() {
		close(done)
		cancel()
	}
}

func checkBackupShutdown(shutdown <-chan struct{}) error {
	if shutdown == nil {
		return nil
	}
	select {
	case <-shutdown:
		return errBackupShutdown
	default:
		return nil
	}
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

type inMemoryBackupProcessor struct {
	srcFiles map[string]*fileData
	dstFiles map[string]*fileData
}

func NewInMemoryBackupProcessor() *inMemoryBackupProcessor {
	return &inMemoryBackupProcessor{
		srcFiles: make(map[string]*fileData),
		dstFiles: make(map[string]*fileData),
	}
}

func (bp *inMemoryBackupProcessor) addSrcFile(checksum string, size int64, backupDest []string, file core.Fpath) error {
	if existing, ok := bp.srcFiles[checksum]; ok {
		merged := mergeBackupDestinations(existing.BackupDest, backupDest)
		if len(backupDest) < len(existing.BackupDest) {
			existing.Fpath = file
			existing.Size = size
		}
		existing.BackupDest = merged
		return nil
	}
	bp.srcFiles[checksum] = &fileData{Size: size, Fpath: file, BackupDest: backupDest}
	return nil
}

func (bp *inMemoryBackupProcessor) addDstFile(checksum string, size int64, backupDest []string, file core.Fpath) error {
	if existing, ok := bp.dstFiles[checksum]; ok {
		existing.BackupDest = mergeBackupDestinations(existing.BackupDest, backupDest)
		return nil
	}
	bp.dstFiles[checksum] = &fileData{Size: size, Fpath: file, BackupDest: backupDest}
	return nil
}

func (bp *inMemoryBackupProcessor) Close() error {
	return nil
}

func (bp *inMemoryBackupProcessor) prioritizedSrcFiles(
	maxNumBackups int,
	shutdown <-chan struct{},
) (func(yield func(fileData) bool) error, error) {
	entries := make([]fileData, 0, len(bp.srcFiles))
	for checksum, source := range bp.srcFiles {
		if _, exists := bp.dstFiles[checksum]; exists || len(source.BackupDest) > maxNumBackups {
			continue
		}
		entries = append(entries, *source)
	}
	sort.Slice(entries, func(i, j int) bool {
		if len(entries[i].BackupDest) != len(entries[j].BackupDest) {
			return len(entries[i].BackupDest) < len(entries[j].BackupDest)
		}
		if entries[i].Size != entries[j].Size {
			return entries[i].Size > entries[j].Size
		}
		return entries[i].Fpath.String() < entries[j].Fpath.String()
	})
	iterator := func(yield func(fileData) bool) error {
		for _, entry := range entries {
			if err := checkBackupShutdown(shutdown); err != nil {
				return err
			}
			if !yield(entry) {
				return nil
			}
		}
		return nil
	}
	return iterator, nil
}
