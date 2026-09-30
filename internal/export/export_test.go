package export

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"trustwallet-etl/internal/store/postgres"
	"trustwallet-etl/internal/transform"
)

// fakeStore is an in-memory Store. It can be told to fail the checkpoint write
// to simulate a crash between file write and commit.
type fakeStore struct {
	raw         []postgres.RawRow
	snaps       []postgres.SnapshotRow
	checkpoints map[string]int64
	failNextSet bool
	setCalls    int
}

func newFakeStore() *fakeStore { return &fakeStore{checkpoints: map[string]int64{}} }

func (f *fakeStore) ListRawFetchesAfter(_ context.Context, after int64, limit int) ([]postgres.RawRow, error) {
	var out []postgres.RawRow
	for _, r := range f.raw {
		if r.ID > after && len(out) < limit {
			out = append(out, r)
		}
	}
	return out, nil
}

func (f *fakeStore) ListSnapshotsAfter(_ context.Context, after int64, limit int) ([]postgres.SnapshotRow, error) {
	var out []postgres.SnapshotRow
	for _, r := range f.snaps {
		if r.ID > after && len(out) < limit {
			out = append(out, r)
		}
	}
	return out, nil
}

func (f *fakeStore) GetCheckpoint(_ context.Context, name string) (int64, error) {
	return f.checkpoints[name], nil
}

func (f *fakeStore) SetCheckpoint(_ context.Context, name string, last int64) error {
	f.setCalls++
	if f.failNextSet {
		f.failNextSet = false
		return errors.New("simulated crash before checkpoint commit")
	}
	f.checkpoints[name] = last
	return nil
}

func day(d int) time.Time { return time.Date(2026, 9, d, 12, 0, 0, 0, time.UTC) }

func rawRow(id int64, at time.Time) postgres.RawRow {
	return postgres.RawRow{ID: id, FetchedAt: at, Source: "binance", Endpoint: "https://api.binance.com/api/v3/ticker/24hr", HTTPStatus: 200,
		Request: json.RawMessage(`{"quote":"USDT"}`), Response: json.RawMessage(`[{"symbol":"BTCUSDT","lastPrice":"100000.00000000"}]`)}
}

func snapRow(id int64, at time.Time) postgres.SnapshotRow {
	return postgres.SnapshotRow{ID: id, Snapshot: transform.Snapshot{RawFetchID: 1, AssetID: "c0", Symbol: "BTC", QuoteCurrency: "USDT", Price: "100000.00000000",
		SourceTime: at.Add(-time.Second), LastTradeID: 6724035014, FetchedAt: at}, LoadedAt: at}
}

func readLines(t *testing.T, path string) []map[string]any {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	var out []map[string]any
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("line is not JSON: %q", sc.Text())
		}
		out = append(out, m)
	}
	return out
}

func TestExportRawPartitionsByFetchedDateAndAppends(t *testing.T) {
	dir := t.TempDir()
	st := newFakeStore()
	st.raw = []postgres.RawRow{rawRow(1, day(28)), rawRow(2, day(29)), rawRow(3, day(29))}
	e := New(st, dir)

	res, err := e.ExportRaw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows != 3 || res.LastID != 3 || len(res.Files) != 2 {
		t.Fatalf("res = %+v", res)
	}
	p28 := filepath.Join(dir, "raw", "ticker_24hr", "dt=2026-09-28", "ticker_24hr.jsonl")
	p29 := filepath.Join(dir, "raw", "ticker_24hr", "dt=2026-09-29", "ticker_24hr.jsonl")
	if got := readLines(t, p28); len(got) != 1 || got[0]["raw_fetch_id"].(float64) != 1 {
		t.Errorf("dt=28: %v", got)
	}
	if got := readLines(t, p29); len(got) != 2 {
		t.Errorf("dt=29: %v", got)
	}
	if st.checkpoints[DatasetRaw] != 3 {
		t.Errorf("checkpoint = %d", st.checkpoints[DatasetRaw])
	}

	// A second run with nothing new must not touch the files.
	res, err = e.ExportRaw(context.Background())
	if err != nil || res.Rows != 0 || len(res.Files) != 0 {
		t.Fatalf("rerun: res=%+v err=%v", res, err)
	}
	if got := readLines(t, p29); len(got) != 2 {
		t.Errorf("rerun appended lines: %d", len(got))
	}

	// New rows are appended, existing lines preserved.
	st.raw = append(st.raw, rawRow(4, day(29)))
	if _, err := e.ExportRaw(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := readLines(t, p29)
	if len(got) != 3 || got[2]["raw_fetch_id"].(float64) != 4 {
		t.Errorf("append: %v", got)
	}
	// The raw line embeds the response as JSON, not as an escaped string.
	if _, ok := got[0]["response"].([]any); !ok {
		t.Errorf("raw line must embed the response object: %v", got[0])
	}
}

// TestExportRawSplitsPairsFromTickers: pair discovery rows, with their large
// exchangeInfo bodies, get their own raw dataset next to the ticker file, and
// one checkpoint still covers both.
func TestExportRawSplitsPairsFromTickers(t *testing.T) {
	dir := t.TempDir()
	st := newFakeStore()
	pairs := rawRow(1, day(29))
	pairs.Kind, pairs.Response = "pairs", json.RawMessage(`{"symbols":[{"symbol":"BTCUSDT"}]}`)
	st.raw = []postgres.RawRow{pairs, rawRow(2, day(29)), rawRow(3, day(29))}

	res, err := New(st, dir).ExportRaw(context.Background())
	if err != nil || res.Rows != 3 || res.LastID != 3 || len(res.Files) != 2 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	pPairs := filepath.Join(dir, "raw", "exchange_info", "dt=2026-09-29", "exchange_info.jsonl")
	pTickers := filepath.Join(dir, "raw", "ticker_24hr", "dt=2026-09-29", "ticker_24hr.jsonl")
	if got := readLines(t, pPairs); len(got) != 1 || got[0]["kind"] != "pairs" {
		t.Errorf("exchange_info: %v", got)
	}
	if got := readLines(t, pTickers); len(got) != 2 || got[0]["raw_fetch_id"].(float64) != 2 {
		t.Errorf("ticker_24hr: %v", got)
	}
	if st.checkpoints[DatasetRaw] != 3 {
		t.Errorf("one checkpoint must cover both files, got %d", st.checkpoints[DatasetRaw])
	}
}

func TestExportProcessedKeepsDecimalStringsAndPartitions(t *testing.T) {
	dir := t.TempDir()
	st := newFakeStore()
	st.snaps = []postgres.SnapshotRow{snapRow(1, day(28)), snapRow(2, day(29))}
	res, err := New(st, dir).ExportProcessed(context.Background())
	if err != nil || res.Rows != 2 || len(res.Files) != 2 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	p28 := filepath.Join(dir, "processed", "price_snapshots", "dt=2026-09-28", "price_snapshots.jsonl")
	got := readLines(t, p28)
	if len(got) != 1 || got[0]["snapshot_id"].(float64) != 1 || got[0]["symbol"] != "BTC" {
		t.Errorf("dt=28: %v", got)
	}
	if got[0]["price"] != "100000.00000000" || got[0]["last_trade_id"].(float64) != 6724035014 || got[0]["source_time"] != "2026-09-28T11:59:59Z" {
		t.Errorf("price must export as a decimal string, with the source clock and trade id: %v", got[0])
	}
	if got[0]["fetched_at"] != "2026-09-28T12:00:00Z" {
		t.Errorf("timestamps must be RFC 3339 UTC: %v", got[0]["fetched_at"])
	}
}

// TestExportCrashBetweenWriteAndCheckpointReplaysBatch documents the
// at-least-once contract: the batch written before a failed checkpoint is
// written again on the next run, so the file holds duplicates and the reader
// must dedupe on the id.
func TestExportCrashBetweenWriteAndCheckpointReplaysBatch(t *testing.T) {
	dir := t.TempDir()
	st := newFakeStore()
	st.raw = []postgres.RawRow{rawRow(1, day(29)), rawRow(2, day(29))}
	st.failNextSet = true
	e := New(st, dir)

	if _, err := e.ExportRaw(context.Background()); err == nil {
		t.Fatal("expected checkpoint failure")
	}
	if st.checkpoints[DatasetRaw] != 0 {
		t.Fatalf("checkpoint must not advance on failure")
	}
	res, err := e.ExportRaw(context.Background())
	if err != nil || res.Rows != 2 {
		t.Fatalf("recovery run: res=%+v err=%v", res, err)
	}
	got := readLines(t, res.Files[0])
	if len(got) != 4 {
		t.Fatalf("expected the batch to be replayed (4 lines), got %d", len(got))
	}
	ids := map[float64]int{}
	for _, m := range got {
		ids[m["raw_fetch_id"].(float64)]++
	}
	if ids[1] != 2 || ids[2] != 2 {
		t.Errorf("each id should appear exactly twice: %v", ids)
	}
}

// TestExportChunksLargeBacklog proves a backlog is read and written in bounded
// chunks with the checkpoint advancing after each durable chunk, so memory
// stays flat however far behind the files are.
func TestExportChunksLargeBacklog(t *testing.T) {
	dir := t.TempDir()
	st := newFakeStore()
	for i := int64(1); i <= 12; i++ {
		st.raw = append(st.raw, rawRow(i, day(29)))
	}
	e := New(st, dir)
	e.chunk = 5

	res, err := e.ExportRaw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows != 12 || res.LastID != 12 || len(res.Files) != 1 {
		t.Fatalf("res = %+v", res)
	}
	if st.setCalls != 3 {
		t.Errorf("expected 3 checkpoint advances for chunks of 5, 5 and 2, got %d", st.setCalls)
	}
	if st.checkpoints[DatasetRaw] != 12 {
		t.Errorf("checkpoint = %d", st.checkpoints[DatasetRaw])
	}
	if got := readLines(t, res.Files[0]); len(got) != 12 {
		t.Errorf("lines = %d", len(got))
	}
}

// TestExportRepairsTruncatedTail: a crash mid-write leaves a partial trailing
// line; the next export truncates back to the last complete record before
// appending, and the affected records re-export whole.
func TestExportRepairsTruncatedTail(t *testing.T) {
	dir := t.TempDir()
	st := newFakeStore()
	st.raw = []postgres.RawRow{rawRow(1, day(29)), rawRow(2, day(29))}
	e := New(st, dir)
	if _, err := e.ExportRaw(context.Background()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "raw", "ticker_24hr", "dt=2026-09-29", "ticker_24hr.jsonl")
	// Simulate a crash mid-write of row 3: a partial line on disk, checkpoint still at 2.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"raw_fetch_id":3,"trunc`); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	st.raw = append(st.raw, rawRow(3, day(29)))

	res, err := e.ExportRaw(context.Background())
	if err != nil || res.Rows != 1 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	got := readLines(t, path) // fails on any line that is not valid JSON
	if len(got) != 3 || got[2]["raw_fetch_id"].(float64) != 3 {
		t.Errorf("lines = %v", got)
	}
}

// TestExportReportsWhenItHitsTheCap: a run that stops at its cap says so, so
// a growing backlog is visible instead of hiding behind a fresh success time.
func TestExportReportsWhenItHitsTheCap(t *testing.T) {
	dir := t.TempDir()
	st := newFakeStore()
	for i := int64(1); i <= 7; i++ {
		st.raw = append(st.raw, rawRow(i, day(29)))
	}
	e := New(st, dir)
	e.chunk, e.cap = 2, 4
	res, err := e.ExportRaw(context.Background())
	if err != nil || res.Rows != 4 || !res.Capped || st.checkpoints[DatasetRaw] != 4 {
		t.Fatalf("capped run: res=%+v err=%v", res, err)
	}
	res, err = e.ExportRaw(context.Background())
	if err != nil || res.Rows != 3 || res.Capped {
		t.Fatalf("draining run: res=%+v err=%v", res, err)
	}
}

func TestExportEmptyStoreWritesNothing(t *testing.T) {
	dir := t.TempDir()
	res, err := New(newFakeStore(), dir).ExportProcessed(context.Background())
	if err != nil || res.Rows != 0 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "processed")); !os.IsNotExist(err) {
		t.Errorf("no directory should be created for an empty export")
	}
}
