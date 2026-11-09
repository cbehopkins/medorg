package consumers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	_ "modernc.org/sqlite"
)

const (
	restoreStatePending = "pending"
	restoreStateCopied  = "copied"
)

type restoreContent struct {
	key         restoreContentKey
	backupDests []string
}

// RestoreDB manages persistent restore targets for one active restore context.
type RestoreDB struct {
	db        *sql.DB
	mu        sync.Mutex
	closed    bool
	closeOnce sync.Once
	closeErr  error
}

// OpenRestoreDB opens or creates a SQLite restore database at the given path.
func OpenRestoreDB(filePath string) (*RestoreDB, error) {
	if err := rejectNonSQLiteRestoreDB(filePath); err != nil {
		return nil, err
	}

	sqlDB, err := sql.Open("sqlite", filePath)
	if err != nil {
		return nil, fmt.Errorf("open restore database: %w", err)
	}
	sqlDB.SetMaxOpenConns(1)
	db := &RestoreDB{db: sqlDB}
	if err := sqlDB.Ping(); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("connect to restore database: %w", err)
	}
	for _, statement := range []string{
		`CREATE TABLE IF NOT EXISTS restore_content (
			md5 TEXT NOT NULL,
			size INTEGER NOT NULL,
			backup_dests TEXT NOT NULL,
			PRIMARY KEY (md5, size)
		)`,
		`CREATE TABLE IF NOT EXISTS restore_targets (
			md5 TEXT NOT NULL,
			size INTEGER NOT NULL,
			target_abs_path TEXT NOT NULL,
			task_id TEXT NOT NULL,
			alias TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			completed_at_unix INTEGER NOT NULL,
			copy_source_path TEXT NOT NULL,
			state TEXT NOT NULL CHECK (state IN ('pending', 'copied')),
			PRIMARY KEY (md5, size, target_abs_path),
			FOREIGN KEY (md5, size) REFERENCES restore_content (md5, size)
		)`,
		`CREATE INDEX IF NOT EXISTS restore_targets_by_state
			ON restore_targets (state, md5, size)`,
	} {
		if _, err := sqlDB.Exec(statement); err != nil {
			_ = sqlDB.Close()
			return nil, fmt.Errorf("initialise restore database: %w", err)
		}
	}
	return db, nil
}

func rejectNonSQLiteRestoreDB(filePath string) error {
	info, err := os.Stat(filePath)
	if os.IsNotExist(err) || (err == nil && info.Size() == 0) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect restore database: %w", err)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("read restore database header: %w", err)
	}
	defer file.Close()

	header := make([]byte, 16)
	read, readErr := io.ReadFull(file, header)
	if readErr != nil || read != len(header) || string(header) != "SQLite format 3\x00" {
		return fmt.Errorf("restore database %q is not SQLite; existing bobbob databases are not supported", filePath)
	}
	return nil
}

// InsertPending adds a target to the pending state, preserving one row per distinct target.
func (db *RestoreDB) InsertPending(target *RestoreTaskTarget, md5 string, size int64, backupDests []string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.ensureOpenLocked(); err != nil {
		return err
	}

	tx, err := db.db.Begin()
	if err != nil {
		return fmt.Errorf("begin pending insert: %w", err)
	}
	defer tx.Rollback()

	content := restoreContent{key: NewRestoreContentKey(md5, size), backupDests: backupDests}
	if err := mergeRestoreContentDestinations(tx, content); err != nil {
		return err
	}
	if _, err := tx.Exec(`
		INSERT INTO restore_targets (
			md5, size, target_abs_path, task_id, alias, created_at_unix,
			updated_at_unix, completed_at_unix, copy_source_path, state
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (md5, size, target_abs_path) DO NOTHING`,
		content.key.MD5, content.key.Size, target.TargetAbsPath, target.TaskID, target.Alias,
		target.CreatedAtUnix, target.UpdatedAtUnix, target.CompletedAtUnix,
		target.CopySourcePath, restoreStatePending); err != nil {
		return fmt.Errorf("insert pending restore target: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit pending restore target: %w", err)
	}
	return nil
}

// InsertCopied adds a target directly to the copied state.
func (db *RestoreDB) InsertCopied(target *RestoreTaskTarget, md5 string, size int64, backupDests []string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.ensureOpenLocked(); err != nil {
		return err
	}

	tx, err := db.db.Begin()
	if err != nil {
		return fmt.Errorf("begin copied insert: %w", err)
	}
	defer tx.Rollback()

	content := restoreContent{key: NewRestoreContentKey(md5, size), backupDests: backupDests}
	if err := mergeRestoreContentDestinations(tx, content); err != nil {
		return err
	}
	if _, err := tx.Exec(`
		INSERT INTO restore_targets (
			md5, size, target_abs_path, task_id, alias, created_at_unix,
			updated_at_unix, completed_at_unix, copy_source_path, state
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (md5, size, target_abs_path) DO UPDATE SET state = excluded.state`,
		content.key.MD5, content.key.Size, target.TargetAbsPath, target.TaskID, target.Alias,
		target.CreatedAtUnix, target.UpdatedAtUnix, target.CompletedAtUnix,
		target.CopySourcePath, restoreStateCopied); err != nil {
		return fmt.Errorf("insert copied restore target: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit copied restore target: %w", err)
	}
	return nil
}

func mergeRestoreContentDestinations(tx *sql.Tx, content restoreContent) error {
	var existingJSON string
	var existing []string
	err := tx.QueryRow(`SELECT backup_dests FROM restore_content WHERE md5 = ? AND size = ?`, content.key.MD5, content.key.Size).Scan(&existingJSON)
	switch {
	case err == nil:
		if err := json.Unmarshal([]byte(existingJSON), &existing); err != nil {
			return fmt.Errorf("decode restore backup destinations: %w", err)
		}
	case err != sql.ErrNoRows:
		return fmt.Errorf("read restore content: %w", err)
	}

	mergedJSON, err := json.Marshal(mergeBackupDestinations(existing, content.backupDests))
	if err != nil {
		return fmt.Errorf("encode restore backup destinations: %w", err)
	}
	if _, err := tx.Exec(`
		INSERT INTO restore_content (md5, size, backup_dests) VALUES (?, ?, ?)
		ON CONFLICT (md5, size) DO UPDATE SET backup_dests = excluded.backup_dests`,
		content.key.MD5, content.key.Size, string(mergedJSON)); err != nil {
		return fmt.Errorf("save restore content: %w", err)
	}
	return nil
}

// FindPendingByContent returns pending targets matching the checksum and size.
func (db *RestoreDB) FindPendingByContent(md5 string, size int64) ([]RestoreTaskTarget, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.ensureOpenLocked(); err != nil {
		return nil, err
	}

	rows, err := db.db.Query(`
		SELECT task_id, alias, target_abs_path, created_at_unix,
			updated_at_unix, completed_at_unix, copy_source_path
		FROM restore_targets
		WHERE md5 = ? AND size = ? AND state = ?
		ORDER BY rowid`, md5, size, restoreStatePending)
	if err != nil {
		return nil, fmt.Errorf("find pending restore targets: %w", err)
	}
	defer rows.Close()

	targets := make([]RestoreTaskTarget, 0)
	for rows.Next() {
		var target RestoreTaskTarget
		if err := rows.Scan(
			&target.TaskID, &target.Alias, &target.TargetAbsPath,
			&target.CreatedAtUnix, &target.UpdatedAtUnix, &target.CompletedAtUnix,
			&target.CopySourcePath,
		); err != nil {
			return nil, fmt.Errorf("read pending restore target: %w", err)
		}
		targets = append(targets, target)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending restore targets: %w", err)
	}
	return targets, nil
}

// MoveToCopied atomically marks one pending target copied after its file copy succeeds.
func (db *RestoreDB) MoveToCopied(targetPath string, md5 string, size int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.ensureOpenLocked(); err != nil {
		return err
	}

	tx, err := db.db.Begin()
	if err != nil {
		return fmt.Errorf("begin restore state transition: %w", err)
	}
	defer tx.Rollback()

	var exists int
	if err := tx.QueryRow(`SELECT 1 FROM restore_content WHERE md5 = ? AND size = ?`, md5, size).Scan(&exists); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("content not found in pending: %s, %d", md5, size)
		}
		return fmt.Errorf("find restore content: %w", err)
	}
	result, err := tx.Exec(`
		UPDATE restore_targets SET state = ?
		WHERE md5 = ? AND size = ? AND target_abs_path = ? AND state = ?`,
		restoreStateCopied, md5, size, targetPath, restoreStatePending)
	if err != nil {
		return fmt.Errorf("move restore target to copied: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check restore state transition: %w", err)
	}
	if updated == 0 {
		return fmt.Errorf("target %s not found in pending", targetPath)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit restore state transition: %w", err)
	}
	return nil
}

// CountPending returns the number of content groups with pending targets.
func (db *RestoreDB) CountPending() (int, error) {
	return db.countContentGroups(restoreStatePending)
}

// CountCopied returns the number of content groups with copied targets.
func (db *RestoreDB) CountCopied() (int, error) {
	return db.countContentGroups(restoreStateCopied)
}

func (db *RestoreDB) countContentGroups(state string) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.ensureOpenLocked(); err != nil {
		return 0, err
	}

	var count int
	err := db.db.QueryRow(`
		SELECT COUNT(*) FROM (
			SELECT md5, size FROM restore_targets WHERE state = ? GROUP BY md5, size
		)`, state).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count %s restore content: %w", state, err)
	}
	return count, nil
}

// CountPendingByBackupDest groups pending target counts by required backup volume.
func (db *RestoreDB) CountPendingByBackupDest() (map[string]int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.ensureOpenLocked(); err != nil {
		return nil, err
	}

	rows, err := db.db.Query(`
		SELECT content.backup_dests, COUNT(*)
		FROM restore_content AS content
		JOIN restore_targets AS target USING (md5, size)
		WHERE target.state = ?
		GROUP BY content.md5, content.size`, restoreStatePending)
	if err != nil {
		return nil, fmt.Errorf("count pending targets by backup volume: %w", err)
	}
	defer rows.Close()

	result := make(map[string]int)
	for rows.Next() {
		var destinationsJSON string
		var targetCount int
		if err := rows.Scan(&destinationsJSON, &targetCount); err != nil {
			return nil, fmt.Errorf("read pending backup volume counts: %w", err)
		}
		var destinations []string
		if err := json.Unmarshal([]byte(destinationsJSON), &destinations); err != nil {
			return nil, fmt.Errorf("decode pending backup volumes: %w", err)
		}
		for _, destination := range destinations {
			result[destination] += targetCount
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending backup volume counts: %w", err)
	}
	return result, nil
}

func (db *RestoreDB) ensureOpenLocked() error {
	if db.closed {
		return fmt.Errorf("RestoreDB is shutting down")
	}
	return nil
}

// Close closes the restore database and releases resources.
func (db *RestoreDB) Close() error {
	db.closeOnce.Do(func() {
		db.mu.Lock()
		defer db.mu.Unlock()
		db.closed = true
		db.closeErr = db.db.Close()
	})
	return db.closeErr
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
