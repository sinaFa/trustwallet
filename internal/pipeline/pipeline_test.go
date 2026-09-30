package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"trustwallet-etl/internal/export"
	"trustwallet-etl/internal/observe"
	"trustwallet-etl/internal/source/binance"
	"trustwallet-etl/internal/store/postgres"
	"trustwallet-etl/internal/transform"
)

// defaultPairs: three verified registry symbols plus GAS, which the source
// trades but the symbol map excludes.
var defaultPairs = []string{"BTCUSDT", "ETHUSDT", "GASUSDT", "SOLUSDT"}

var assetIDs = map[string]string{"BTC": "c0", "ETH": "c60", "SOL": "c501"}

// fakeFetcher answers both requests. pairs nil means the default four; an
// explicit empty slice means nothing trades. delay makes FetchTickers take
// that long while honouring ctx, and started signals when it begins.
type fakeFetcher struct {
	pairs    []string
	pairsErr error
	res      *binance.FetchResult
	err      error
	delay    time.Duration
	started  chan struct{}
	resolves int
	gotPairs []string
}

func (f *fakeFetcher) ResolvePairs(context.Context) (*binance.PairsResult, error) {
	f.resolves++
	if f.pairsErr != nil {
		return nil, f.pairsErr
	}
	pairs := f.pairs
	if pairs == nil {
		pairs = defaultPairs
	}
	return &binance.PairsResult{Call: binance.Call{
		Kind: binance.KindPairs, Endpoint: "https://api.binance.com/api/v3/exchangeInfo", Request: json.RawMessage(`{"query":{"permissions":"SPOT"}}`),
		StatusCode: 200, Body: []byte(`{"symbols":[]}`), Attempts: 1, Duration: 300 * time.Millisecond, FetchedAt: fetchedAt,
	}, Pairs: pairs}, nil
}

func (f *fakeFetcher) FetchTickers(ctx context.Context, pairs []string) (*binance.FetchResult, error) {
	f.gotPairs = pairs
	if f.started != nil {
		f.started <- struct{}{}
	}
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return nil, &binance.Error{Call: binance.Call{Kind: binance.KindTickers, Request: json.RawMessage(`{}`), FetchedAt: fetchedAt}, Retryable: true, Err: ctx.Err()}
		}
	}
	return f.res, f.err
}

type fakeStore struct {
	raw      []postgres.RawFetch
	snaps    [][]transform.Snapshot
	replaced []int64 // raw fetch ids whose rows a replay rewrote
	latest   map[string]string
	rawRows  []postgres.RawRow
	rawErr   error
	insErr   error
	pingErr  error
	// lockChecks counts WriterLockHeld calls; from the lockFailAt-th on, the lock is gone.
	lockChecks, lockFailAt int
}

func (s *fakeStore) InsertRawFetch(_ context.Context, r postgres.RawFetch) (int64, error) {
	if s.rawErr != nil {
		return 0, s.rawErr
	}
	s.raw = append(s.raw, r)
	return int64(len(s.raw)), nil
}

func (s *fakeStore) InsertSnapshots(_ context.Context, snaps []transform.Snapshot) (postgres.InsertStats, error) {
	if s.insErr != nil {
		return postgres.InsertStats{}, s.insErr
	}
	s.snaps = append(s.snaps, snaps)
	return postgres.InsertStats{Inserted: len(snaps)}, nil
}

func (s *fakeStore) ReplaceSnapshots(_ context.Context, rawID int64, snaps []transform.Snapshot) (postgres.InsertStats, error) {
	s.replaced = append(s.replaced, rawID)
	s.snaps = append(s.snaps, snaps)
	return postgres.InsertStats{Inserted: len(snaps), Deleted: 1}, nil
}

func (s *fakeStore) LatestPrices(context.Context) (map[string]string, error) { return s.latest, nil }

func (s *fakeStore) ListRawFetchesAfter(_ context.Context, after int64, limit int) ([]postgres.RawRow, error) {
	var out []postgres.RawRow
	for _, r := range s.rawRows {
		if r.ID > after && len(out) < limit {
			out = append(out, r)
		}
	}
	return out, nil
}

func (s *fakeStore) Ping(context.Context) error { return s.pingErr }

func (s *fakeStore) WriterLockHeld(context.Context) error {
	s.lockChecks++
	if s.lockFailAt > 0 && s.lockChecks >= s.lockFailAt {
		return errors.New("lock connection closed")
	}
	return nil
}

type fakeExporter struct {
	rawCalls, procCalls int
	capped              bool
	err                 error
}

func (e *fakeExporter) ExportRaw(context.Context) (export.Result, error) {
	e.rawCalls++
	return export.Result{Rows: 1, Capped: e.capped}, e.err
}

func (e *fakeExporter) ExportProcessed(context.Context) (export.Result, error) {
	e.procCalls++
	return export.Result{Rows: 2}, e.err
}

var fetchedAt = time.Date(2026, 9, 29, 16, 30, 0, 0, time.UTC)

// tickerBody renders a ticker response from pair, price pairs; every pair last
// traded at fetchedAt.
func tickerBody(pairPrices ...string) string {
	var parts []string
	for i := 0; i+1 < len(pairPrices); i += 2 {
		parts = append(parts, fmt.Sprintf(`{"symbol":%q,"lastPrice":%q,"closeTime":%d,"lastId":%d}`,
			pairPrices[i], pairPrices[i+1], fetchedAt.UnixMilli(), 1000+i))
	}
	return "[" + strings.Join(parts, ",") + "]"
}

// okBody: BTC loads; ETH has a zero price and is dropped; SOL is absent;
// BTCUSDC is another quote and is ignored.
var okBody = tickerBody("BTCUSDT", "100000", "ETHUSDT", "0", "BTCUSDC", "99990")

func result(body string) *binance.FetchResult {
	var tickers []binance.Ticker
	_ = json.Unmarshal([]byte(body), &tickers)
	return &binance.FetchResult{Call: binance.Call{
		Kind: binance.KindTickers, Endpoint: "https://api.binance.com/api/v3/ticker/24hr", Request: json.RawMessage(`{"quote":"USDT"}`),
		StatusCode: 200, Body: []byte(body), Attempts: 2, Duration: 120 * time.Millisecond, FetchedAt: fetchedAt,
	}, Tickers: tickers}
}

// tickersError is a failed ticker request as the client reports it.
func tickersError(status, code int, body string) *binance.Error {
	return &binance.Error{Call: binance.Call{
		Kind: binance.KindTickers, Endpoint: "https://api.binance.com/api/v3/ticker/24hr", Request: json.RawMessage(`{"quote":"USDT"}`),
		StatusCode: status, Body: []byte(body), Attempts: 3, Duration: 900 * time.Millisecond, FetchedAt: fetchedAt,
	}, Code: code, Retryable: status >= 500, Err: fmt.Errorf("http %d", status)}
}

type harness struct {
	p    *Pipeline
	st   *fakeStore
	ex   *fakeExporter
	logs *bytes.Buffer
}

func newHarness(f Fetcher) *harness {
	logs := &bytes.Buffer{}
	st := &fakeStore{}
	ex := &fakeExporter{}
	cfg := Config{Interval: 30 * time.Second, ReadinessMaxAge: 90 * time.Second, DrainTimeout: time.Second,
		SourceName: "binance", Quote: "USDT", Symbols: []string{"BTC", "ETH", "GAS", "SOL"}, AssetIDs: assetIDs, Excluded: []string{"GAS"}}
	p := New(cfg, f, st, ex, slog.New(slog.NewJSONHandler(logs, nil)), observe.NewMetrics())
	p.now = func() time.Time { return fetchedAt.Add(time.Second) }
	return &harness{p: p, st: st, ex: ex, logs: logs}
}

// advance moves the fake clock forward by d.
func (h *harness) advance(d time.Duration) {
	now := h.p.now()
	h.p.now = func() time.Time { return now.Add(d) }
}

// logLines returns every log event with the given msg, in order.
func (h *harness) logLines(t *testing.T, msg string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(h.logs.String()), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not JSON: %s", line)
		}
		if m["msg"] == msg {
			out = append(out, m)
		}
	}
	return out
}

func (h *harness) hasLog(t *testing.T, msg string) map[string]any {
	t.Helper()
	lines := h.logLines(t, msg)
	if len(lines) == 0 {
		t.Fatalf("no log line with msg %q; got %s", msg, h.logs.String())
	}
	return lines[0]
}

func TestRunOnceHappyPathLogsEveryStage(t *testing.T) {
	f := &fakeFetcher{res: result(okBody)}
	h := newHarness(f)
	if err := h.p.RunOnce(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// One raw row per HTTP request: pair discovery first, then the tickers,
	// stored before any interpretation. Snapshots descend from the tickers row.
	if len(h.st.raw) != 2 {
		t.Fatalf("raw rows = %d, want pairs and tickers", len(h.st.raw))
	}
	if r := h.st.raw[0]; r.Kind != binance.KindPairs || r.Outcome != "success" || r.RequestedCount != 4 || r.ReturnedCount != 4 || !strings.HasSuffix(r.Endpoint, "/exchangeInfo") {
		t.Errorf("pairs row = %+v", r)
	}
	if r := h.st.raw[1]; r.Kind != binance.KindTickers || string(r.Response) != okBody || r.RequestedCount != 3 || r.ReturnedCount != 3 || r.Attempts != 2 {
		t.Errorf("tickers row = %+v", r)
	}
	// Only verified pairs are requested: GASUSDT trades, but the map excludes it.
	if strings.Join(f.gotPairs, ",") != "BTCUSDT,ETHUSDT,SOLUSDT" {
		t.Errorf("requested pairs = %v", f.gotPairs)
	}
	// BTC loads with its asset id; ETH has a zero price and is dropped; SOL is missing; BTCUSDC is ignored.
	if len(h.st.snaps) != 1 || len(h.st.snaps[0]) != 1 || h.st.snaps[0][0].Symbol != "BTC" || h.st.snaps[0][0].AssetID != "c0" || h.st.snaps[0][0].RawFetchID != 2 || h.st.snaps[0][0].Price != "100000" {
		t.Errorf("snaps = %+v", h.st.snaps)
	}
	if h.ex.rawCalls != 1 || h.ex.procCalls != 1 {
		t.Errorf("exports: raw=%d processed=%d", h.ex.rawCalls, h.ex.procCalls)
	}

	if l := h.hasLog(t, "trading pairs resolved"); l["pairs"].(float64) != 3 || l["excluded"].(float64) != 1 || l["unverified"].(float64) != 0 {
		t.Errorf("resolved log = %v", l)
	}
	if l := h.hasLog(t, "registry symbols the source trades under a pair the symbol map excludes; not priced"); l["symbols"].([]any)[0] != "GAS" {
		t.Errorf("excluded log = %v", l)
	}
	api := h.logLines(t, "api request succeeded")
	if len(api) != 2 || api[0]["kind"] != "pairs" || api[1]["kind"] != "tickers" || api[1]["attempts"].(float64) != 2 || api[1]["returned"].(float64) != 3 {
		t.Errorf("api logs = %v", api)
	}
	terr := h.hasLog(t, "transformation error")
	if terr["symbol"] != "ETH" || terr["field"] != "price" {
		t.Errorf("transform error log = %v", terr)
	}
	gap := h.hasLog(t, "source coverage baseline: registry symbols the source does not quote")
	if gap["missing"].(float64) != 2 || gap["priced"].(float64) != 1 {
		t.Errorf("coverage log = %v", gap)
	}
	saved := h.hasLog(t, "data saved successfully")
	if saved["inserted"].(float64) != 1 {
		t.Errorf("saved log = %v", saved)
	}
	h.hasLog(t, "data exported successfully")
	h.hasLog(t, "run completed")
	if _, err := h.p.Ready(context.Background()); err != nil {
		t.Errorf("should be ready after a success: %v", err)
	}

	// The pairs are kept: the second run makes one request, not two.
	if err := h.p.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.resolves != 1 || len(h.st.raw) != 3 || h.st.raw[2].Kind != binance.KindTickers {
		t.Errorf("second run: resolves=%d raw rows=%d", f.resolves, len(h.st.raw))
	}
}

// TestPairsAreResolvedAgainAfterAnUnknownPair: Binance reporting an unknown
// pair fails that poll loudly, keeps its JSON error body in the raw layer, and
// makes a later poll rebuild the pair list after one interval of backoff.
func TestPairsAreResolvedAgainAfterAnUnknownPair(t *testing.T) {
	f := &fakeFetcher{res: result(okBody)}
	h := newHarness(f)
	if err := h.p.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.err = tickersError(400, binance.CodeInvalidSymbol, `{"code":-1121,"msg":"Invalid symbol."}`)
	if err := h.p.RunOnce(context.Background()); err == nil {
		t.Fatal("expected the poll to fail")
	}
	h.hasLog(t, "a requested pair is no longer listed; pairs will be resolved again")
	if r := h.st.raw[len(h.st.raw)-1]; r.Kind != binance.KindTickers || r.Outcome != "failure" || r.HTTPStatus != 400 || string(r.Response) != `{"code":-1121,"msg":"Invalid symbol."}` {
		t.Errorf("failure row must keep the JSON error body: %+v", r)
	}
	f.err = nil
	h.advance(time.Minute)
	if err := h.p.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.resolves != 2 {
		t.Errorf("pairs must be resolved again after -1121, resolves=%d", f.resolves)
	}
}

// TestPairDiscoveryFailureIsRecordedAndBacksOff: the exchangeInfo request is
// recorded under its own endpoint and kind; the next poll does not repeat it
// until a backoff has passed, then tries again and succeeds.
func TestPairDiscoveryFailureIsRecordedAndBacksOff(t *testing.T) {
	f := &fakeFetcher{res: result(okBody)}
	f.pairsErr = &binance.Error{Call: binance.Call{
		Kind: binance.KindPairs, Endpoint: "https://api.binance.com/api/v3/exchangeInfo", Request: json.RawMessage(`{"query":{}}`),
		StatusCode: 503, Attempts: 2, Duration: 8 * time.Second, FetchedAt: fetchedAt,
	}, Retryable: true, Err: errors.New("http 503")}
	h := newHarness(f)
	if err := h.p.RunOnce(context.Background()); err == nil {
		t.Fatal("expected the poll to fail")
	}
	if len(h.st.raw) != 1 || h.st.raw[0].Kind != binance.KindPairs || h.st.raw[0].Outcome != "failure" || h.st.raw[0].HTTPStatus != 503 || !strings.HasSuffix(h.st.raw[0].Endpoint, "/exchangeInfo") {
		t.Errorf("raw = %+v", h.st.raw)
	}
	if l := h.hasLog(t, "api request failed"); l["kind"] != "pairs" || l["attempts"].(float64) != 2 {
		t.Errorf("log = %v", l)
	}
	if f.gotPairs != nil {
		t.Errorf("tickers must not be requested without pairs")
	}
	// Same clock: the poll fails fast without a second discovery request.
	f.pairsErr = nil
	if err := h.p.RunOnce(context.Background()); err == nil || !strings.Contains(err.Error(), "backed off") || f.resolves != 1 || len(h.st.raw) != 1 {
		t.Fatalf("backed-off poll: err=%v resolves=%d raw=%d", err, f.resolves, len(h.st.raw))
	}
	h.hasLog(t, "pair discovery backed off after a failure or an empty result")
	// One interval later, discovery runs again.
	h.advance(31 * time.Second)
	if err := h.p.RunOnce(context.Background()); err != nil || f.resolves != 2 {
		t.Fatalf("next poll: err=%v resolves=%d", err, f.resolves)
	}
}

// TestNoVerifiedPairsFailsTheRunLoudlyAndBacksOff: an empty verified list is
// recorded as a successful request, fails the run, and doubles the wait before
// the next discovery up to ten minutes, so a misconfigured quote asset costs
// one request every ten minutes rather than one every poll.
func TestNoVerifiedPairsFailsTheRunLoudlyAndBacksOff(t *testing.T) {
	f := &fakeFetcher{res: result(okBody), pairs: []string{"GASUSDT"}}
	h := newHarness(f)
	if err := h.p.RunOnce(context.Background()); err == nil || !strings.Contains(err.Error(), "no verified registry symbol trades against USDT") {
		t.Fatalf("err = %v", err)
	}
	if len(h.st.raw) != 1 || h.st.raw[0].Kind != binance.KindPairs || h.st.raw[0].Outcome != "success" || h.st.raw[0].ReturnedCount != 1 {
		t.Errorf("raw = %+v", h.st.raw)
	}
	h.hasLog(t, "no verified registry symbol trades against the quote asset")
	if f.gotPairs != nil {
		t.Errorf("tickers must not be requested for no pairs")
	}
	waits := []time.Duration{30 * time.Second, 60 * time.Second, 120 * time.Second, 240 * time.Second, 480 * time.Second, 600 * time.Second, 600 * time.Second}
	for i, want := range waits {
		if got := h.p.discoveryBackoff; got != want {
			t.Fatalf("attempt %d: backoff %s, want %s", i+1, got, want)
		}
		h.advance(want)
		_ = h.p.RunOnce(context.Background())
	}
	if f.resolves != len(waits)+1 {
		t.Errorf("resolves = %d", f.resolves)
	}
}

// TestExcludedSymbolsAreNotUnverified: a symbol the map reviewed and excluded
// is not the same signal as one it has no row for. Only the second is a new
// listing waiting for a review, so only the second warns and counts as
// unverified; neither is requested.
func TestExcludedSymbolsAreNotUnverified(t *testing.T) {
	f := &fakeFetcher{res: result(okBody), pairs: []string{"BTCUSDT", "GASUSDT", "XYZUSDT"}}
	h := newHarness(f)
	if err := h.p.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Join(f.gotPairs, ",") != "BTCUSDT" {
		t.Errorf("requested pairs = %v", f.gotPairs)
	}
	if l := h.hasLog(t, "trading pairs resolved"); l["pairs"].(float64) != 1 || l["excluded"].(float64) != 1 || l["unverified"].(float64) != 1 {
		t.Errorf("resolved log = %v", l)
	}
	if l := h.hasLog(t, "registry symbols the source trades that the symbol map has no row for; not priced until reviewed"); l["symbols"].([]any)[0] != "XYZ" {
		t.Errorf("unverified log = %v", l)
	}
	if l := h.hasLog(t, "registry symbols the source trades under a pair the symbol map excludes; not priced"); l["symbols"].([]any)[0] != "GAS" {
		t.Errorf("excluded log = %v", l)
	}
}

// TestCoverageDropIsWarnedOnlyWhenItChanges runs four polls: baseline, an
// identical repeat (no warning, no movement), a price move, then a drop.
func TestCoverageDropIsWarnedOnlyWhenItChanges(t *testing.T) {
	f := &fakeFetcher{res: result(okBody)}
	h := newHarness(f)
	run := func() {
		t.Helper()
		if err := h.p.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	run()
	run()
	if drops := h.logLines(t, "source coverage dropped: symbols priced last run are missing now"); len(drops) != 0 {
		t.Fatalf("no drop should be reported while coverage is stable: %v", drops)
	}
	moved := h.hasLog(t, "price movement since previous run")
	if moved["changed"].(float64) != 0 || moved["unchanged"].(float64) != 1 {
		t.Errorf("identical runs must report zero movement: %v", moved)
	}

	f.res = result(tickerBody("BTCUSDT", "50000", "ETHUSDT", "0"))
	run()
	moves := h.logLines(t, "price movement since previous run")
	if last := moves[len(moves)-1]; last["changed"].(float64) != 1 {
		t.Errorf("BTC price changed, expected changed=1: %v", last)
	}

	f.res = result(tickerBody("SOLUSDT", "100", "BTCUSDC", "99990"))
	run()
	drop := h.hasLog(t, "source coverage dropped: symbols priced last run are missing now")
	if drop["dropped"].(float64) != 1 {
		t.Errorf("drop log = %v", drop)
	}
}

// TestRunOnceEmptyResponseIsRecordedNotFatal: a 200 with no tickers is a real
// observation of an empty source. The raw row is kept, every symbol is
// reported missing, no snapshots are written, and the run still succeeds so
// the coverage metrics can tell the story rather than an outage alert.
func TestRunOnceEmptyResponseIsRecordedNotFatal(t *testing.T) {
	h := newHarness(&fakeFetcher{res: result(`[]`)})
	if err := h.p.RunOnce(context.Background()); err != nil {
		t.Fatalf("empty response must not fail the run: %v", err)
	}
	if len(h.st.raw) != 2 || h.st.raw[1].ReturnedCount != 0 {
		t.Errorf("raw rows must be kept: %+v", h.st.raw)
	}
	if len(h.st.snaps) != 1 || len(h.st.snaps[0]) != 0 {
		t.Errorf("no snapshots expected: %+v", h.st.snaps)
	}
	base := h.hasLog(t, "source coverage baseline: registry symbols the source does not quote")
	if base["missing"].(float64) != 4 || base["priced"].(float64) != 0 {
		t.Errorf("coverage log = %v", base)
	}
	if saved := h.hasLog(t, "data saved successfully"); saved["inserted"].(float64) != 0 {
		t.Errorf("saved log = %v", saved)
	}
}

func TestRunOnceFetchFailureIsRecordedNotProcessed(t *testing.T) {
	h := newHarness(&fakeFetcher{err: tickersError(503, 0, "")})
	if err := h.p.RunOnce(context.Background()); err == nil {
		t.Fatal("expected error")
	}
	l := h.hasLog(t, "api request failed")
	if l["kind"] != "tickers" || l["status"].(float64) != 503 || l["attempts"].(float64) != 3 {
		t.Errorf("log = %v", l)
	}
	h.hasLog(t, "run failed")
	// The failure itself lands in the raw layer next to the pairs row; nothing downstream runs.
	if len(h.st.raw) != 2 {
		t.Fatalf("raw rows = %d", len(h.st.raw))
	}
	if r := h.st.raw[1]; r.Kind != binance.KindTickers || r.Outcome != "failure" || r.HTTPStatus != 503 || r.Error == "" || r.Response != nil || r.Attempts != 3 {
		t.Errorf("failure must be recorded in raw: %+v", r)
	}
	if len(h.st.snaps) != 0 || h.ex.rawCalls != 0 {
		t.Errorf("nothing downstream should run on fetch failure")
	}
	if _, err := h.p.Ready(context.Background()); err == nil {
		t.Errorf("must not be ready before any success")
	}
}

// TestRunOnceStageFailures: a failure at any stage after the fetch fails the
// run, logs its own event, stops the later stages, and leaves readiness false.
func TestRunOnceStageFailures(t *testing.T) {
	for name, tc := range map[string]struct {
		arrange func(h *harness)
		msg     string
		check   func(h *harness) bool
	}{
		"raw store down": {func(h *harness) { h.st.rawErr = errors.New("db down") }, "raw save failed",
			func(h *harness) bool { return len(h.st.snaps) == 0 && h.ex.rawCalls == 0 }},
		"insert rejected": {func(h *harness) { h.st.insErr = errors.New("constraint") }, "data save failed",
			func(h *harness) bool { return h.ex.rawCalls == 0 }},
		"export disk full": {func(h *harness) { h.ex.err = errors.New("disk full") }, "export failed",
			func(h *harness) bool { return true }},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(&fakeFetcher{res: result(okBody)})
			tc.arrange(h)
			if err := h.p.RunOnce(context.Background()); err == nil {
				t.Fatal("expected error")
			}
			h.hasLog(t, tc.msg)
			if !tc.check(h) {
				t.Errorf("later stages must not run after %q", tc.msg)
			}
			if _, err := h.p.Ready(context.Background()); err == nil {
				t.Errorf("a failed run is not a success")
			}
		})
	}
}

// TestExportCapIsReported: an export that stopped at its cap is counted and
// warned about, so a backlog never hides behind a fresh success timestamp.
func TestExportCapIsReported(t *testing.T) {
	h := newHarness(&fakeFetcher{res: result(okBody)})
	h.ex.capped = true
	if err := h.p.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if l := h.hasLog(t, "export stopped at its per-run cap; a backlog remains for the next run"); l["dataset"] != export.DatasetRaw {
		t.Errorf("cap log = %v", l)
	}
}

// TestPriceJumpIsFlagged: a price that moves more than 10x between consecutive
// polls is warned and counted, and still loads. Validity says positive,
// plausibility says bounded, and the snapshot records what the source said.
func TestPriceJumpIsFlagged(t *testing.T) {
	f := &fakeFetcher{res: result(okBody)} // BTC at 100000
	h := newHarness(f)
	if err := h.p.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.res = result(tickerBody("BTCUSDT", "1000000000")) // a 10000x move
	if err := h.p.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	l := h.hasLog(t, "price jump between consecutive polls")
	if l["symbol"] != "BTC" || l["previous"] != "100000" || l["current"] != "1000000000" {
		t.Errorf("jump log = %v", l)
	}
	if len(h.st.snaps[1]) != 1 {
		t.Errorf("the flagged row must still load: %+v", h.st.snaps[1])
	}
}

// TestCoverageBaselineSurvivesRestart: a fresh pipeline restores the previous
// run's priced set from the store, so a drop across a restart still warns.
func TestCoverageBaselineSurvivesRestart(t *testing.T) {
	h := newHarness(&fakeFetcher{res: result(okBody)})
	h.st.latest = map[string]string{"BTC": "100000", "SOL": "117"}
	if err := h.p.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	h.hasLog(t, "coverage baseline restored from the previous run")
	drop := h.hasLog(t, "source coverage dropped: symbols priced last run are missing now")
	if drop["dropped"].(float64) != 1 {
		t.Errorf("SOL was priced before the restart and is missing now: %v", drop)
	}
}

// TestReplayRewritesARangeOfRawFetches: every ticker response in the range has
// its rows replaced, so a transform fix corrects wrong prices, not only adds
// missing ones; rows past the upper bound, pair discovery rows, failures and
// undecodable bodies are left alone, the last of them loudly.
func TestReplayRewritesARangeOfRawFetches(t *testing.T) {
	h := newHarness(&fakeFetcher{})
	h.st.rawRows = []postgres.RawRow{
		{ID: 6, Kind: binance.KindPairs, FetchedAt: fetchedAt, Response: json.RawMessage(`{"symbols":[]}`)},
		{ID: 7, Kind: binance.KindTickers, FetchedAt: fetchedAt, Response: json.RawMessage(tickerBody("BTCUSDT", "100000", "SOLUSDT", "100"))},
		{ID: 8, Kind: binance.KindTickers, FetchedAt: fetchedAt, Response: json.RawMessage(`not json`)},
		{ID: 9, Kind: binance.KindTickers, FetchedAt: fetchedAt, Outcome: "failure"},
		{ID: 10, Kind: binance.KindTickers, FetchedAt: fetchedAt, Response: json.RawMessage(tickerBody("BTCUSDT", "100001"))},
		{ID: 11, Kind: binance.KindTickers, FetchedAt: fetchedAt, Response: json.RawMessage(tickerBody("BTCUSDT", "100002"))},
	}
	if err := h.p.Replay(context.Background(), 0, 10); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(h.st.replaced) != "[7 10]" {
		t.Fatalf("rewritten raw fetches = %v, want [7 10]", h.st.replaced)
	}
	if len(h.st.snaps) != 2 || len(h.st.snaps[0]) != 2 || h.st.snaps[0][0].AssetID != "c0" {
		t.Fatalf("snaps = %+v", h.st.snaps)
	}
	if s := h.st.snaps[0][0]; s.RawFetchID != 7 || !s.FetchedAt.Equal(fetchedAt) {
		t.Errorf("replayed rows must keep their original lineage: %+v", s)
	}
	h.hasLog(t, "replay skipped an undecodable raw response")
	if done := h.hasLog(t, "replay completed"); done["raw_fetches"].(float64) != 5 || done["inserted"].(float64) != 3 || done["deleted"].(float64) != 2 || done["errors"].(float64) != 1 {
		t.Errorf("summary = %v", done)
	}
}

func TestReadyReflectsStalenessAndDatabase(t *testing.T) {
	h := newHarness(&fakeFetcher{res: result(okBody)})
	if err := h.p.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	h.p.now = func() time.Time { return fetchedAt.Add(10 * time.Minute) }
	if _, err := h.p.Ready(context.Background()); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("stale success should fail readiness, got %v", err)
	}
	h.p.now = func() time.Time { return fetchedAt.Add(2 * time.Second) }
	h.st.pingErr = errors.New("connection refused")
	if _, err := h.p.Ready(context.Background()); err == nil || !strings.Contains(err.Error(), "database") {
		t.Errorf("db failure should fail readiness, got %v", err)
	}
}

func TestRunStopsOnContextCancel(t *testing.T) {
	h := newHarness(&fakeFetcher{res: result(okBody)})
	h.p.cfg.Interval = 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Millisecond)
	defer cancel()
	if err := h.p.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v", err)
	}
	if n := len(h.logLines(t, "run completed")); n < 3 {
		t.Errorf("expected several runs, got %d", n)
	}
	h.hasLog(t, "pipeline stopping")
}

// TestRunStopsWhenTheWriterLockIsLost: the lock is checked before every cycle;
// once it is gone the loop stops with an error instead of writing unlocked.
func TestRunStopsWhenTheWriterLockIsLost(t *testing.T) {
	h := newHarness(&fakeFetcher{res: result(okBody)})
	h.p.cfg.Interval = 10 * time.Millisecond
	h.st.lockFailAt = 2
	if err := h.p.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "writer lock") {
		t.Fatalf("err = %v", err)
	}
	if n := len(h.logLines(t, "run completed")); n != 1 {
		t.Errorf("one run before the lock check failed, got %d", n)
	}
	h.hasLog(t, "writer lock lost; stopping so a restart can take it again")
}

// runInBackground starts Run and returns a way to wait for it with a bound.
func runInBackground(t *testing.T, h *harness, ctx context.Context) func() error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- h.p.Run(ctx) }()
	return func() error {
		select {
		case err := <-done:
			return err
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not return")
			return nil
		}
	}
}

// TestShutdownDrainsTheRunInFlight: a shutdown signal mid-fetch lets the fetch
// finish, reach the raw table, load and export, and only then stops the loop.
func TestShutdownDrainsTheRunInFlight(t *testing.T) {
	f := &fakeFetcher{res: result(okBody), delay: 60 * time.Millisecond, started: make(chan struct{}, 1)}
	h := newHarness(f)
	ctx, cancel := context.WithCancel(context.Background())
	wait := runInBackground(t, h, ctx)
	<-f.started
	cancel()
	if err := wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if len(h.st.raw) != 2 || len(h.st.snaps) != 1 || h.ex.rawCalls != 1 {
		t.Errorf("the run in flight must complete: raw=%d snaps=%d exports=%d", len(h.st.raw), len(h.st.snaps), h.ex.rawCalls)
	}
	h.hasLog(t, "shutdown requested, finishing the run in flight")
	h.hasLog(t, "run completed")
	h.hasLog(t, "pipeline stopping")
	if len(h.logLines(t, "run failed")) != 0 {
		t.Errorf("a drained run must not fail")
	}
}

// TestShutdownDrainIsBounded: a run that will not finish is cut at the drain
// timeout, recorded as a failure, and the loop still stops promptly.
func TestShutdownDrainIsBounded(t *testing.T) {
	f := &fakeFetcher{res: result(okBody), delay: 10 * time.Second, started: make(chan struct{}, 1)}
	h := newHarness(f)
	h.p.cfg.DrainTimeout = 30 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	wait := runInBackground(t, h, ctx)
	<-f.started
	start := time.Now()
	cancel()
	if err := wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("drain must be bounded, took %s", took)
	}
	h.hasLog(t, "drain timeout reached, cutting the run in flight")
	h.hasLog(t, "run failed")
	if r := h.st.raw[len(h.st.raw)-1]; r.Kind != binance.KindTickers || r.Outcome != "failure" {
		t.Errorf("the cut fetch must be recorded: %+v", r)
	}
	if len(h.st.snaps) != 0 {
		t.Errorf("nothing downstream after a cut fetch")
	}
}

// TestCycleIsCutAtTheInterval: whatever the source does, one cycle never runs
// past one poll interval, so a slow request can never delay the next tick.
func TestCycleIsCutAtTheInterval(t *testing.T) {
	f := &fakeFetcher{res: result(okBody), delay: 10 * time.Second}
	h := newHarness(f)
	h.p.cfg.Interval = 40 * time.Millisecond
	start := time.Now()
	err := h.p.RunOnce(context.Background())
	if took := time.Since(start); err == nil || took > 2*time.Second {
		t.Fatalf("err=%v took=%s", err, took)
	}
	h.hasLog(t, "run failed")
	if r := h.st.raw[len(h.st.raw)-1]; r.Outcome != "failure" || r.Kind != binance.KindTickers {
		t.Errorf("the cut request must be recorded: %+v", r)
	}
}
