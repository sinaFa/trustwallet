// Package postgres persists the asset registry, raw fetches and price snapshots.
// Write semantics per table are in the README's data model section.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"trustwallet-etl/internal/registry"
	"trustwallet-etl/internal/transform"
)

// writerLock is the session advisory lock every writing process holds for its
// lifetime, so two of them never append to the same files and advance the
// same checkpoints at once. Distinct from the migration lock.
const writerLock int64 = 0x7477_6574_6c02

type Store struct {
	pool *pgxpool.Pool
	// lock is the connection holding the writer lock, nil until acquired.
	lock *pgxpool.Conn
}

// Open connects and verifies the connection.
func Open(ctx context.Context, url string) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Store) Close() { s.pool.Close() }

// AcquireWriterLock takes the writer lock on a dedicated connection and holds
// it until the returned release is called. A second writer, whether another
// service replica or an -export-only run next to a live service, fails at
// once instead of doubling every exported line.
func (s *Store) AcquireWriterLock(ctx context.Context) (release func(), err error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection: %w", err)
	}
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, writerLock).Scan(&got); err != nil {
		conn.Release()
		return nil, fmt.Errorf("writer lock: %w", err)
	}
	if !got {
		conn.Release()
		return nil, errors.New("another writer holds the lock: stop the running service before -export-only or -replay-after")
	}
	s.lock = conn
	return func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, writerLock)
		conn.Release()
		s.lock = nil
	}, nil
}

// WriterLockHeld reports whether the writer lock is still held. A session lock
// dies with its connection, so after a Postgres restart the pool would carry
// on writing over fresh connections without it; the pipeline asks before
// every cycle and stops when the answer is no.
func (s *Store) WriterLockHeld(ctx context.Context) error {
	if s.lock == nil {
		return errors.New("writer lock not acquired")
	}
	if err := s.lock.Ping(ctx); err != nil {
		return fmt.Errorf("lock connection lost: %w", err)
	}
	return nil
}

// execBatch runs a batch inside one transaction and hands each command's
// result, with its position, to onResult, so callers only describe their
// statements and counting.
func (s *Store) execBatch(ctx context.Context, batch *pgx.Batch, onResult func(i int, ct pgconn.CommandTag)) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	results := tx.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		ct, err := results.Exec()
		if err != nil {
			results.Close()
			return fmt.Errorf("statement %d: %w", i+1, err)
		}
		onResult(i, ct)
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("close batch: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// UpsertAssets loads the registry in one transaction. Existing rows are
// updated in place and revived if previously removed, so a refreshed snapshot
// takes effect on restart. Rows absent from the snapshot are stamped
// removed_at, never deleted: historical prices still join to them. Returns
// rows loaded and rows newly stamped as removed.
func (s *Store) UpsertAssets(ctx context.Context, assets []registry.Asset) (loaded, removed int, err error) {
	batch := &pgx.Batch{}
	ids := make([]string, 0, len(assets))
	for _, a := range assets {
		ids = append(ids, a.AssetID)
		var contract *string
		if a.ContractAddress != "" {
			c := a.ContractAddress
			contract = &c
		}
		batch.Queue(`
			INSERT INTO assets (asset_id, coin_id, chain, contract_address, symbol, name, decimals, token_type, loaded_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
			ON CONFLICT (asset_id) DO UPDATE SET
				coin_id = EXCLUDED.coin_id, chain = EXCLUDED.chain, contract_address = EXCLUDED.contract_address,
				symbol = EXCLUDED.symbol, name = EXCLUDED.name, decimals = EXCLUDED.decimals,
				token_type = EXCLUDED.token_type, loaded_at = now(), removed_at = NULL`,
			a.AssetID, a.CoinID, a.Chain, contract, a.Symbol, a.Name, a.Decimals, a.TokenType)
	}
	batch.Queue(`UPDATE assets SET removed_at = now() WHERE removed_at IS NULL AND asset_id != ALL($1)`, ids)
	err = s.execBatch(ctx, batch, func(i int, ct pgconn.CommandTag) {
		if i < len(assets) {
			loaded += int(ct.RowsAffected())
		} else {
			removed = int(ct.RowsAffected())
		}
	})
	if err != nil {
		return loaded, removed, fmt.Errorf("upsert assets: %w", err)
	}
	return loaded, removed, nil
}

// RawFetch is one HTTP request to record, success or failure. Kind says which
// of the source's requests it was. A failure has http status 0 when no
// response arrived, Error set, and a Response only when the source sent a JSON
// error body.
type RawFetch struct {
	FetchedAt      time.Time
	Source         string
	Kind           string
	Endpoint       string
	HTTPStatus     int
	Request        json.RawMessage
	Response       json.RawMessage
	Duration       time.Duration
	Attempts       int
	RequestedCount int
	ReturnedCount  int
	Outcome        string
	Error          string
}

// InsertRawFetch appends a raw fetch and returns its id. Kind and Outcome are
// checked by the schema, so a caller that leaves one empty fails loudly here.
func (s *Store) InsertRawFetch(ctx context.Context, r RawFetch) (int64, error) {
	var errText *string
	if r.Error != "" {
		errText = &r.Error
	}
	var id int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO raw_fetches
			(fetched_at, source, kind, endpoint, http_status, request, response, duration_ms, attempts, requested_count, returned_count, outcome, error)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		RETURNING id`,
		r.FetchedAt.UTC(), r.Source, r.Kind, r.Endpoint, r.HTTPStatus, r.Request, r.Response,
		r.Duration.Milliseconds(), r.Attempts, r.RequestedCount, r.ReturnedCount, r.Outcome, errText,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert raw_fetch: %w", err)
	}
	return id, nil
}

// InsertStats reports what a write did, for metrics and logs.
type InsertStats struct {
	Inserted int
	Skipped  int // already present: a replay of the same raw fetch
	Deleted  int // rows a rewrite removed first
}

func queueSnapshots(batch *pgx.Batch, snaps []transform.Snapshot) {
	for _, sn := range snaps {
		batch.Queue(`
			INSERT INTO price_snapshots (raw_fetch_id, asset_id, symbol, quote_currency, price, source_time, last_trade_id, fetched_at)
			VALUES ($1, $2, $3, $4, $5::numeric, $6, $7, $8)
			ON CONFLICT (raw_fetch_id, symbol) DO NOTHING`,
			sn.RawFetchID, sn.AssetID, sn.Symbol, sn.QuoteCurrency, sn.Price, sn.SourceTime.UTC(), sn.LastTradeID, sn.FetchedAt.UTC())
	}
}

// InsertSnapshots writes snapshots atomically, skipping rows whose stable key
// already exists.
func (s *Store) InsertSnapshots(ctx context.Context, snaps []transform.Snapshot) (InsertStats, error) {
	var stats InsertStats
	if len(snaps) == 0 {
		return stats, nil
	}
	batch := &pgx.Batch{}
	queueSnapshots(batch, snaps)
	err := s.execBatch(ctx, batch, func(_ int, ct pgconn.CommandTag) {
		if ct.RowsAffected() == 1 {
			stats.Inserted++
		} else {
			stats.Skipped++
		}
	})
	if err != nil {
		return stats, fmt.Errorf("insert snapshots: %w", err)
	}
	return stats, nil
}

// ReplaceSnapshots rewrites one raw fetch's rows: the existing rows are deleted
// and the given ones inserted in the same transaction, so a replay after a
// transform fix corrects what it wrote before.
func (s *Store) ReplaceSnapshots(ctx context.Context, rawFetchID int64, snaps []transform.Snapshot) (InsertStats, error) {
	var stats InsertStats
	batch := &pgx.Batch{}
	batch.Queue(`DELETE FROM price_snapshots WHERE raw_fetch_id = $1`, rawFetchID)
	queueSnapshots(batch, snaps)
	err := s.execBatch(ctx, batch, func(i int, ct pgconn.CommandTag) {
		if i == 0 {
			stats.Deleted = int(ct.RowsAffected())
		} else {
			stats.Inserted += int(ct.RowsAffected())
		}
	})
	if err != nil {
		return stats, fmt.Errorf("replace snapshots of raw_fetch %d: %w", rawFetchID, err)
	}
	return stats, nil
}

// RawRow is a raw fetch read back for export. Response is nil and Error set
// for failure rows.
type RawRow struct {
	ID             int64           `json:"raw_fetch_id"`
	FetchedAt      time.Time       `json:"fetched_at"`
	Source         string          `json:"source"`
	Kind           string          `json:"kind"`
	Endpoint       string          `json:"endpoint"`
	HTTPStatus     int             `json:"http_status"`
	DurationMs     int             `json:"duration_ms"`
	Attempts       int             `json:"attempts"`
	RequestedCount int             `json:"requested_count"`
	ReturnedCount  int             `json:"returned_count"`
	Outcome        string          `json:"outcome"`
	Error          *string         `json:"error,omitempty"`
	Request        json.RawMessage `json:"request"`
	Response       json.RawMessage `json:"response"`
}

// ListRawFetchesAfter returns up to limit raw fetches with id > afterID, ascending.
func (s *Store) ListRawFetchesAfter(ctx context.Context, afterID int64, limit int) ([]RawRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, fetched_at, source, kind, endpoint, http_status, duration_ms, attempts, requested_count, returned_count, outcome, error, request, response
		FROM raw_fetches WHERE id > $1 ORDER BY id LIMIT $2`, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("list raw_fetches: %w", err)
	}
	defer rows.Close()
	var out []RawRow
	for rows.Next() {
		var r RawRow
		if err := rows.Scan(&r.ID, &r.FetchedAt, &r.Source, &r.Kind, &r.Endpoint, &r.HTTPStatus, &r.DurationMs, &r.Attempts, &r.RequestedCount, &r.ReturnedCount, &r.Outcome, &r.Error, &r.Request, &r.Response); err != nil {
			return nil, fmt.Errorf("scan raw_fetch: %w", err)
		}
		r.FetchedAt = r.FetchedAt.UTC()
		out = append(out, r)
	}
	return out, rows.Err()
}

// SnapshotRow is a processed row read back for export.
type SnapshotRow struct {
	ID int64 `json:"snapshot_id"`
	transform.Snapshot
	LoadedAt time.Time `json:"loaded_at"`
}

// ListSnapshotsAfter returns up to limit snapshots with id > afterID, ascending.
// The price is read as text so no precision is lost on the way out.
func (s *Store) ListSnapshotsAfter(ctx context.Context, afterID int64, limit int) ([]SnapshotRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, raw_fetch_id, asset_id, symbol, quote_currency, price::text, source_time, last_trade_id, fetched_at, loaded_at
		FROM price_snapshots WHERE id > $1 ORDER BY id LIMIT $2`, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("list price_snapshots: %w", err)
	}
	defer rows.Close()
	var out []SnapshotRow
	for rows.Next() {
		var r SnapshotRow
		if err := rows.Scan(&r.ID, &r.RawFetchID, &r.AssetID, &r.Symbol, &r.QuoteCurrency, &r.Price, &r.SourceTime, &r.LastTradeID, &r.FetchedAt, &r.LoadedAt); err != nil {
			return nil, fmt.Errorf("scan price_snapshot: %w", err)
		}
		r.SourceTime, r.FetchedAt, r.LoadedAt = r.SourceTime.UTC(), r.FetchedAt.UTC(), r.LoadedAt.UTC()
		out = append(out, r)
	}
	return out, rows.Err()
}

// LatestPrices returns symbol to price for the newest raw fetch that produced
// snapshots. The pipeline restores its coverage baseline from it at startup,
// so a drop that spans a restart still warns.
func (s *Store) LatestPrices(ctx context.Context) (map[string]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT symbol, price::text FROM price_snapshots
		WHERE raw_fetch_id = (SELECT max(raw_fetch_id) FROM price_snapshots)`)
	if err != nil {
		return nil, fmt.Errorf("latest prices: %w", err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var sym, price string
		if err := rows.Scan(&sym, &price); err != nil {
			return nil, fmt.Errorf("scan latest price: %w", err)
		}
		out[sym] = price
	}
	return out, rows.Err()
}

// GetCheckpoint returns the last exported id for name, or 0 if none.
func (s *Store) GetCheckpoint(ctx context.Context, name string) (int64, error) {
	var last int64
	err := s.pool.QueryRow(ctx, `SELECT last_id FROM export_checkpoints WHERE name = $1`, name).Scan(&last)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get checkpoint %s: %w", name, err)
	}
	return last, nil
}

// SetCheckpoint records the last exported id for name.
func (s *Store) SetCheckpoint(ctx context.Context, name string, lastID int64) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO export_checkpoints (name, last_id, updated_at) VALUES ($1, $2, now())
		ON CONFLICT (name) DO UPDATE SET last_id = EXCLUDED.last_id, updated_at = now()`, name, lastID)
	if err != nil {
		return fmt.Errorf("set checkpoint %s: %w", name, err)
	}
	return nil
}
