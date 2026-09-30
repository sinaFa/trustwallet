//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"trustwallet-etl/internal/registry"
	"trustwallet-etl/internal/transform"
	"trustwallet-etl/migrations"
)

// These tests need a real Postgres. Run with:
//
//	TEST_DATABASE_URL=postgres://etl:etl@localhost:5432/etl?sslmode=disable go test -tags=integration ./internal/store/...
//
// Each test gets its own schema, selected through the connection's
// search_path so every pooled connection uses it, and never touches
// pipeline data.

var schemaSeq atomic.Int64

func testURL(t *testing.T) string {
	t.Helper()
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	return base
}

func openTestSchema(t *testing.T, migrate bool) *Store {
	t.Helper()
	base := testURL(t)
	ctx := context.Background()
	schema := fmt.Sprintf("it_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	admin, err := Open(ctx, base)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := admin.pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	sep := "?"
	if strings.Contains(base, "?") {
		sep = "&"
	}
	st, err := Open(ctx, base+sep+"options=-c%20search_path%3D"+schema)
	if err != nil {
		t.Fatalf("open with search_path: %v", err)
	}
	t.Cleanup(func() {
		st.Close()
		_, _ = admin.pool.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})
	if migrate {
		if _, err := st.Migrate(ctx, migrations.FS); err != nil {
			t.Fatalf("migrate: %v", err)
		}
	}
	return st
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	return openTestSchema(t, true)
}

func count(t *testing.T, st *Store, table string) int64 {
	t.Helper()
	var n int64
	if err := st.pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func migrationFiles(t *testing.T) []string {
	t.Helper()
	names, err := fs.Glob(migrations.FS, "*.sql")
	if err != nil || len(names) == 0 {
		t.Fatalf("migrations: %v", err)
	}
	return names
}

var testAssets = []registry.Asset{
	{AssetID: "c0", CoinID: 0, Chain: "bitcoin", Symbol: "BTC", Name: "Bitcoin", Decimals: 8, TokenType: "NATIVE"},
	{AssetID: "c60_t0xshib", CoinID: 60, Chain: "ethereum", ContractAddress: "0xshib", Symbol: "SHIB", Name: "SHIBA INU", Decimals: 18, TokenType: "ERC20"},
}

func TestMigrateRecordsVersionsAndIsIdempotent(t *testing.T) {
	st := openTestStore(t)
	if n, want := count(t, st, "schema_migrations"), int64(len(migrationFiles(t))); n != want {
		t.Fatalf("recorded %d versions, want %d", n, want)
	}
	applied, err := st.Migrate(context.Background(), migrations.FS)
	if err != nil || len(applied) != 0 {
		t.Fatalf("second run must apply nothing: applied=%v err=%v", applied, err)
	}
}

// TestMigrateRefusesAChangedFile: a file edited after it ran no longer matches
// the database, and the service must not start on that.
func TestMigrateRefusesAChangedFile(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	if _, err := st.pool.Exec(ctx, `UPDATE schema_migrations SET checksum = 'tampered' WHERE version = '001_init.sql'`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Migrate(ctx, migrations.FS); err == nil || !strings.Contains(err.Error(), "changed after it was applied") {
		t.Fatalf("err = %v", err)
	}
}

// TestMigrateAdoptsAnUnversionedDatabase: a database with the tables already
// in place but no schema_migrations, built by hand here, is adopted on the
// next start: every file applies once more, harmlessly, and is recorded.
func TestMigrateAdoptsAnUnversionedDatabase(t *testing.T) {
	st := openTestSchema(t, false)
	ctx := context.Background()
	names := migrationFiles(t)
	for _, name := range names {
		sql, err := fs.ReadFile(migrations.FS, name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply %s by hand: %v", name, err)
		}
	}
	applied, err := st.Migrate(ctx, migrations.FS)
	if err != nil || len(applied) != len(names) {
		t.Fatalf("adopt: applied=%v err=%v", applied, err)
	}
	if n := count(t, st, "schema_migrations"); n != int64(len(names)) {
		t.Errorf("recorded %d versions", n)
	}
}

// TestMigrateSerialisesConcurrentStarts: two replicas starting on a fresh
// database at once both succeed, and every version is recorded exactly once,
// because the advisory lock makes the second wait for the first.
func TestMigrateSerialisesConcurrentStarts(t *testing.T) {
	st := openTestSchema(t, false)
	ctx := context.Background()
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, err := st.Migrate(ctx, migrations.FS)
			errs <- err
		}()
	}
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if n, want := count(t, st, "schema_migrations"), int64(len(migrationFiles(t))); n != want {
		t.Errorf("recorded %d versions, want %d", n, want)
	}
}

// TestWriterLockAdmitsOneWriter: a second process cannot take the writer lock
// while the first holds it, and can once the first releases. A lock whose
// session died, which is what a Postgres restart does to it, is reported as
// lost rather than assumed.
func TestWriterLockAdmitsOneWriter(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	other, err := Open(ctx, testURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()

	if err := st.WriterLockHeld(ctx); err == nil {
		t.Fatal("nothing acquired yet must not count as held")
	}
	release, err := st.AcquireWriterLock(ctx)
	if err != nil {
		t.Fatalf("first writer: %v", err)
	}
	if err := st.WriterLockHeld(ctx); err != nil {
		t.Fatalf("held: %v", err)
	}
	if _, err := other.AcquireWriterLock(ctx); err == nil || !strings.Contains(err.Error(), "another writer holds the lock") {
		t.Fatalf("second writer must be refused, got %v", err)
	}
	release()
	release2, err := other.AcquireWriterLock(ctx)
	if err != nil {
		t.Fatalf("after release: %v", err)
	}
	// Kill the holder's session from the outside; the lock goes with it.
	if _, err := st.pool.Exec(ctx, `SELECT pg_terminate_backend($1)`, int32(other.lock.Conn().PgConn().PID())); err != nil {
		t.Fatal(err)
	}
	if err := other.WriterLockHeld(ctx); err == nil || !strings.Contains(err.Error(), "lock connection lost") {
		t.Fatalf("a dead session must be reported as a lost lock, got %v", err)
	}
	release3, err := st.AcquireWriterLock(ctx)
	if err != nil {
		t.Fatalf("the lock must be free once its session died: %v", err)
	}
	release3()
	release2()
}

func TestUpsertAssetsIsIdempotentAndUpdates(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	assets := []registry.Asset{
		{AssetID: "c60", CoinID: 60, Chain: "ethereum", Symbol: "ETH", Name: "Ethereum", Decimals: 18, TokenType: "NATIVE"},
		{AssetID: "c60_t0xabc", CoinID: 60, Chain: "ethereum", ContractAddress: "0xabc", Symbol: "TKN", Name: "Token", Decimals: 6, TokenType: "ERC20"},
	}
	if n, removed, err := st.UpsertAssets(ctx, assets); err != nil || n != 2 || removed != 0 {
		t.Fatalf("first load: n=%d removed=%d err=%v", n, removed, err)
	}
	assets[1].Name = "Token v2"
	if _, _, err := st.UpsertAssets(ctx, assets); err != nil {
		t.Fatal(err)
	}
	var name string
	var contract *string
	if err := st.pool.QueryRow(ctx, `SELECT name, contract_address FROM assets WHERE asset_id = 'c60_t0xabc'`).Scan(&name, &contract); err != nil {
		t.Fatal(err)
	}
	if name != "Token v2" || contract == nil || *contract != "0xabc" {
		t.Errorf("name=%q contract=%v", name, contract)
	}
	if n := count(t, st, "assets"); n != 2 {
		t.Errorf("assets=%d", n)
	}

	// Dropping an asset from the snapshot stamps it; re-adding revives it.
	var removedAt *time.Time
	if _, removed, err := st.UpsertAssets(ctx, assets[:1]); err != nil || removed != 1 {
		t.Fatalf("shrunk load: removed=%d err=%v", removed, err)
	}
	if err := st.pool.QueryRow(ctx, `SELECT removed_at FROM assets WHERE asset_id = 'c60_t0xabc'`).Scan(&removedAt); err != nil || removedAt == nil {
		t.Fatalf("removed_at should be set, got %v err=%v", removedAt, err)
	}
	if _, removed, err := st.UpsertAssets(ctx, assets); err != nil || removed != 0 {
		t.Fatalf("revive: removed=%d err=%v", removed, err)
	}
	if err := st.pool.QueryRow(ctx, `SELECT removed_at FROM assets WHERE asset_id = 'c60_t0xabc'`).Scan(&removedAt); err != nil || removedAt != nil {
		t.Fatalf("removed_at should be cleared, got %v err=%v", removedAt, err)
	}
}

// TestRawFetchFailureRowRoundTrips: the raw layer records failures with their
// kind, the JSON error body when the source sent one, and the error text, and
// they read back for export the same way. A row without a kind is refused by
// the schema rather than defaulted.
func TestRawFetchFailureRowRoundTrips(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	body := json.RawMessage(`{"code": -1121, "msg": "Invalid symbol."}`)
	id, err := st.InsertRawFetch(ctx, RawFetch{
		FetchedAt: time.Now().UTC(), Source: "binance", Kind: "tickers", Endpoint: "https://api.binance.com/api/v3/ticker/24hr",
		HTTPStatus: 400, Request: json.RawMessage(`{"quote":"USDT"}`), Response: body, Attempts: 1,
		RequestedCount: 62, Outcome: "failure", Error: "http 400: Invalid symbol.",
	})
	if err != nil || id == 0 {
		t.Fatalf("insert failure row: id=%d err=%v", id, err)
	}
	if _, err := st.InsertRawFetch(ctx, RawFetch{
		FetchedAt: time.Now().UTC(), Source: "binance", Kind: "pairs", Endpoint: "https://api.binance.com/api/v3/exchangeInfo",
		HTTPStatus: 0, Request: json.RawMessage(`{"query":{}}`), Attempts: 2, RequestedCount: 292, Outcome: "failure", Error: "timeout",
	}); err != nil {
		t.Fatalf("insert bodiless failure row: %v", err)
	}
	if _, err := st.InsertRawFetch(ctx, RawFetch{FetchedAt: time.Now().UTC(), Source: "binance", Endpoint: "e", Request: json.RawMessage(`{}`), Outcome: "success"}); err == nil {
		t.Errorf("a row without a kind must be refused by the schema")
	}
	rows, err := st.ListRawFetchesAfter(ctx, 0, 10)
	if err != nil || len(rows) != 2 {
		t.Fatalf("list: n=%d err=%v", len(rows), err)
	}
	r := rows[0]
	var got map[string]any
	if r.Kind != "tickers" || r.Outcome != "failure" || r.Error == nil || *r.Error != "http 400: Invalid symbol." || r.HTTPStatus != 400 ||
		json.Unmarshal(r.Response, &got) != nil || got["code"].(float64) != -1121 {
		t.Errorf("row = %+v", r)
	}
	if r := rows[1]; r.Kind != "pairs" || r.Response != nil || r.HTTPStatus != 0 {
		t.Errorf("bodiless row = %+v", r)
	}
}

func TestRawInsertAndSnapshotReplayIsIdempotent(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	at := time.Date(2026, 9, 29, 16, 30, 0, 0, time.UTC)
	if _, _, err := st.UpsertAssets(ctx, testAssets); err != nil {
		t.Fatal(err)
	}

	rawID, err := st.InsertRawFetch(ctx, RawFetch{
		FetchedAt: at, Source: "binance", Kind: "tickers", Endpoint: "https://api.binance.com/api/v3/ticker/24hr", HTTPStatus: 200,
		Request:  json.RawMessage(`{"quote":"USDT"}`),
		Response: json.RawMessage(`[{"symbol":"BTCUSDT","lastPrice":"83286.02000000","closeTime":1790753370001,"lastId":6724035014}]`),
		Duration: 120 * time.Millisecond, Attempts: 1, RequestedCount: 2, ReturnedCount: 1, Outcome: "success",
	})
	if err != nil || rawID == 0 {
		t.Fatalf("raw insert: id=%d err=%v", rawID, err)
	}

	btcTime := at.Add(-time.Second)
	snaps := []transform.Snapshot{
		{RawFetchID: rawID, AssetID: "c0", Symbol: "BTC", QuoteCurrency: "USDT", Price: "83286.02000000", SourceTime: btcTime, LastTradeID: 6724035014, FetchedAt: at},
		{RawFetchID: rawID, AssetID: "c60_t0xshib", Symbol: "SHIB", QuoteCurrency: "USDT", Price: "0.00001234", SourceTime: at.Add(-2 * time.Second), LastTradeID: 1, FetchedAt: at},
	}
	first, err := st.InsertSnapshots(ctx, snaps)
	if err != nil || first.Inserted != 2 || first.Skipped != 0 {
		t.Fatalf("first insert: %+v err=%v", first, err)
	}
	// Replaying the same fetch must change nothing.
	second, err := st.InsertSnapshots(ctx, snaps)
	if err != nil || second.Inserted != 0 || second.Skipped != 2 {
		t.Fatalf("replay: %+v err=%v", second, err)
	}
	if raw, sn := count(t, st, "raw_fetches"), count(t, st, "price_snapshots"); raw != 1 || sn != 2 {
		t.Errorf("raw=%d snapshots=%d", raw, sn)
	}

	// NUMERIC keeps the price exactly as sent, scale included; the identity,
	// the source clock and the trade id survive the round trip.
	rows, err := st.ListSnapshotsAfter(ctx, 0, 10)
	if err != nil || len(rows) != 2 {
		t.Fatalf("list: n=%d err=%v", len(rows), err)
	}
	if rows[0].Price != "83286.02000000" || rows[1].Price != "0.00001234" || rows[0].AssetID != "c0" || rows[1].AssetID != "c60_t0xshib" {
		t.Errorf("round trip: %+v %+v", rows[0], rows[1])
	}
	if !rows[0].SourceTime.Equal(btcTime) || rows[0].SourceTime.Location() != time.UTC || rows[0].LastTradeID != 6724035014 {
		t.Errorf("source clock round trip: %+v", rows[0])
	}
	if rows[0].ID >= rows[1].ID || rows[0].FetchedAt.Location() != time.UTC {
		t.Errorf("ordering or tz wrong: %+v", rows)
	}

	// The coverage baseline reads the newest fetch's prices back by symbol.
	latest, err := st.LatestPrices(ctx)
	if err != nil || latest["BTC"] != "83286.02000000" || latest["SHIB"] != "0.00001234" || len(latest) != 2 {
		t.Errorf("latest prices = %v err=%v", latest, err)
	}

	// A snapshot pointing at a raw fetch or an asset that does not exist is rejected.
	if _, err := st.InsertSnapshots(ctx, []transform.Snapshot{{RawFetchID: 9999, AssetID: "c0", Symbol: "X", QuoteCurrency: "USDT", Price: "1", SourceTime: at, FetchedAt: at}}); err == nil {
		t.Errorf("foreign key should reject an orphan snapshot")
	}
	if _, err := st.InsertSnapshots(ctx, []transform.Snapshot{{RawFetchID: rawID, AssetID: "c999", Symbol: "X", QuoteCurrency: "USDT", Price: "1", SourceTime: at, FetchedAt: at}}); err == nil {
		t.Errorf("foreign key should reject an unknown asset")
	}
	// A non-positive price is rejected by the CHECK constraint.
	if _, err := st.InsertSnapshots(ctx, []transform.Snapshot{{RawFetchID: rawID, AssetID: "c0", Symbol: "BAD", QuoteCurrency: "USDT", Price: "0", SourceTime: at, FetchedAt: at}}); err == nil {
		t.Errorf("check constraint should reject zero price")
	}

	// A rewrite replaces the fetch's rows in one transaction: the corrected
	// price lands, the row that no longer transforms is gone.
	rewritten, err := st.ReplaceSnapshots(ctx, rawID, snaps[:1])
	if err != nil || rewritten.Deleted != 2 || rewritten.Inserted != 1 {
		t.Fatalf("rewrite: %+v err=%v", rewritten, err)
	}
	if sn := count(t, st, "price_snapshots"); sn != 1 {
		t.Errorf("after rewrite: snapshots=%d", sn)
	}
}

func TestCheckpoints(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	if v, err := st.GetCheckpoint(ctx, "raw_fetches"); err != nil || v != 0 {
		t.Fatalf("initial: v=%d err=%v", v, err)
	}
	if err := st.SetCheckpoint(ctx, "raw_fetches", 10); err != nil {
		t.Fatal(err)
	}
	if err := st.SetCheckpoint(ctx, "raw_fetches", 25); err != nil {
		t.Fatal(err)
	}
	if v, _ := st.GetCheckpoint(ctx, "raw_fetches"); v != 25 {
		t.Errorf("v = %d", v)
	}
	if v, _ := st.GetCheckpoint(ctx, "price_snapshots"); v != 0 {
		t.Errorf("other dataset must be independent, got %d", v)
	}
}
