package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"sort"

	"github.com/jackc/pgx/v5"
)

// migrationLock is the advisory lock every migrator takes first, so replicas
// starting together apply the schema one at a time instead of racing.
const migrationLock int64 = 0x7477_6574_6c01

// Migrate applies every embedded *.sql file not yet recorded in
// schema_migrations, in lexical order, inside one transaction held under an
// advisory lock. Each applied file's checksum is recorded, and a file that
// changes after it ran is refused on the next start, because the database
// would no longer match the source. A database whose tables already exist but
// were never recorded is adopted: the files apply once more, which the
// idempotent DDL tolerates, and are recorded from then on. Returns the files
// applied this time.
func (s *Store) Migrate(ctx context.Context, migrations fs.FS) (applied []string, err error) {
	names, err := fs.Glob(migrations, "*.sql")
	if err != nil {
		return nil, fmt.Errorf("list migrations: %w", err)
	}
	if len(names) == 0 {
		return nil, errors.New("no migrations found")
	}
	sort.Strings(names)

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLock); err != nil {
		return nil, fmt.Errorf("lock migrations: %w", err)
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    TEXT        PRIMARY KEY,
		checksum   TEXT        NOT NULL,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return nil, fmt.Errorf("create schema_migrations: %w", err)
	}

	applied = []string{}
	for _, name := range names {
		sqlBytes, err := fs.ReadFile(migrations, name)
		if err != nil {
			return applied, fmt.Errorf("read %s: %w", name, err)
		}
		sum := sha256.Sum256(sqlBytes)
		checksum := hex.EncodeToString(sum[:])

		var recorded string
		err = tx.QueryRow(ctx, `SELECT checksum FROM schema_migrations WHERE version = $1`, name).Scan(&recorded)
		switch {
		case err == nil:
			if recorded != checksum {
				return applied, fmt.Errorf("migration %s changed after it was applied: checksum %s, recorded %s", name, checksum, recorded)
			}
			continue
		case !errors.Is(err, pgx.ErrNoRows):
			return applied, fmt.Errorf("check %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
			return applied, fmt.Errorf("apply %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, checksum) VALUES ($1, $2)`, name, checksum); err != nil {
			return applied, fmt.Errorf("record %s: %w", name, err)
		}
		applied = append(applied, name)
	}
	if err := tx.Commit(ctx); err != nil {
		return applied, fmt.Errorf("commit: %w", err)
	}
	return applied, nil
}
