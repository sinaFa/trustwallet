// Package pipeline runs the fetch, store, transform, load, export cycle on a
// fixed interval and reports every stage through logs and metrics.
package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"trustwallet-etl/internal/export"
	"trustwallet-etl/internal/observe"
	"trustwallet-etl/internal/source/binance"
	"trustwallet-etl/internal/store/postgres"
	"trustwallet-etl/internal/transform"
)

type Fetcher interface {
	ResolvePairs(ctx context.Context) (*binance.PairsResult, error)
	FetchTickers(ctx context.Context, pairs []string) (*binance.FetchResult, error)
}

type Store interface {
	InsertRawFetch(ctx context.Context, r postgres.RawFetch) (int64, error)
	InsertSnapshots(ctx context.Context, snaps []transform.Snapshot) (postgres.InsertStats, error)
	ReplaceSnapshots(ctx context.Context, rawFetchID int64, snaps []transform.Snapshot) (postgres.InsertStats, error)
	LatestPrices(ctx context.Context) (map[string]string, error)
	ListRawFetchesAfter(ctx context.Context, afterID int64, limit int) ([]postgres.RawRow, error)
	Ping(ctx context.Context) error
	// WriterLockHeld fails once the writer lock is gone, which a Postgres
	// restart causes, since a session lock dies with its connection.
	WriterLockHeld(ctx context.Context) error
}

type Exporter interface {
	ExportRaw(ctx context.Context) (export.Result, error)
	ExportProcessed(ctx context.Context) (export.Result, error)
}

// priceJumpFactor flags a price that moved more than this factor between two
// consecutive polls. Validity checks say a price is positive; this says it is
// plausible. Flagged rows still load: the snapshot records what the source said.
const priceJumpFactor = 10

// replayChunk bounds one read while replaying raw fetches.
const replayChunk = 1_000

// maxDiscoveryBackoff caps how long the pipeline waits before asking for the
// pair list again after discovery failed or found nothing. The wait doubles
// from one poll interval, so a misconfigured quote asset or a market-wide
// halt costs one discovery request every ten minutes, not one every poll.
const maxDiscoveryBackoff = 10 * time.Minute

type Config struct {
	Interval        time.Duration
	ReadinessMaxAge time.Duration
	// DrainTimeout is how long a run in flight may keep going after a
	// shutdown signal before it is cut.
	DrainTimeout time.Duration
	SourceName   string
	Quote        string
	// Symbols is the upper-cased list of registry symbols to keep.
	Symbols []string
	// AssetIDs binds a symbol to the registry asset it was verified against.
	// Symbols without a binding are counted, never priced.
	AssetIDs map[string]string
	// Excluded are symbols the map reviewed and rejected, told apart from the
	// ones it has no row for: only the latter is a new listing to review.
	Excluded []string
}

// Pipeline owns one polling loop.
type Pipeline struct {
	cfg      Config
	fetcher  Fetcher
	store    Store
	exporter Exporter
	log      *slog.Logger
	metrics  *observe.Metrics
	now      func() time.Time

	lastSuccess atomic.Int64 // unix nanos of the last fully successful run, 0 if none
	runs        atomic.Int64
	// pairs is the registry's verified trading pairs, resolved on the first
	// run and again after the source reports one it no longer knows.
	pairs []string
	// discoveryRetryAt and discoveryBackoff throttle repeated discovery after
	// a failure or an empty result.
	discoveryRetryAt time.Time
	discoveryBackoff time.Duration
	// lastPrices maps the symbols the previous run priced to their price, so a
	// run can report coverage drops and whether any price actually moved.
	lastPrices map[string]string
}

func New(cfg Config, fetcher Fetcher, store Store, exporter Exporter, log *slog.Logger, metrics *observe.Metrics) *Pipeline {
	return &Pipeline{cfg: cfg, fetcher: fetcher, store: store, exporter: exporter, log: log, metrics: metrics, now: time.Now}
}

// Run polls immediately, then on every tick until ctx is cancelled. Runs never
// overlap: the loop is sequential, and a run longer than the interval is counted
// as an overrun rather than started concurrently. Cancellation stops the loop
// after the run in flight has drained, see RunOnce. Losing the writer lock
// stops it too, before the next cycle, so a restart takes the lock again
// rather than the service writing without it.
func (p *Pipeline) Run(ctx context.Context) error {
	p.log.Info("pipeline started", "interval", p.cfg.Interval.String(), "symbols", len(p.cfg.Symbols), "verified", len(p.cfg.AssetIDs))
	ticker := time.NewTicker(p.cfg.Interval)
	defer ticker.Stop()

	for {
		if err := p.store.WriterLockHeld(ctx); err != nil && ctx.Err() == nil {
			p.log.Error("writer lock lost; stopping so a restart can take it again", "error", err.Error())
			return fmt.Errorf("writer lock: %w", err)
		}
		_ = p.RunOnce(ctx) // failures are logged and counted inside; the loop goes on
		select {
		case <-ctx.Done():
			p.log.Info("pipeline stopping", "reason", ctx.Err().Error())
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// RunOnce executes one full cycle. Errors are logged and counted here as well
// as returned, so callers only need the error to decide whether to exit.
//
// Cancelling ctx does not cut the cycle: the work runs on a context that
// outlives ctx by up to DrainTimeout, so a shutdown signal lets a fetch finish
// and reach the raw table, and a database write complete, before the loop
// stops. A cycle also never runs longer than one poll interval.
func (p *Pipeline) RunOnce(ctx context.Context) error {
	drain, cancelDrain := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelDrain()
	stop := context.AfterFunc(ctx, func() {
		p.log.Info("shutdown requested, finishing the run in flight", "drain_timeout", p.cfg.DrainTimeout.String())
		timer := time.NewTimer(p.cfg.DrainTimeout)
		defer timer.Stop()
		select {
		case <-timer.C:
			p.log.Warn("drain timeout reached, cutting the run in flight")
			cancelDrain()
		case <-drain.Done():
		}
	})
	defer stop()
	run := drain
	if p.cfg.Interval > 0 {
		var cancelRun context.CancelFunc
		run, cancelRun = context.WithTimeout(drain, p.cfg.Interval)
		defer cancelRun()
	}

	p.restoreBaseline(run)
	start := p.now()
	log := p.log.With("run_id", p.runs.Add(1))

	err := p.cycle(run, log)

	elapsed := p.now().Sub(start)
	if elapsed > p.cfg.Interval {
		p.metrics.RunOverrunsTotal.Inc()
		log.Warn("run overran the poll interval", "elapsed", elapsed.String(), "interval", p.cfg.Interval.String())
	}
	if err != nil {
		p.metrics.RunsTotal.WithLabelValues("failure").Inc()
		log.Error("run failed", "error", err.Error(), "elapsed", elapsed.String())
		return err
	}
	p.metrics.RunsTotal.WithLabelValues("success").Inc()
	p.lastSuccess.Store(p.now().UnixNano())
	log.Info("run completed", "elapsed", elapsed.String())
	return nil
}

func (p *Pipeline) cycle(ctx context.Context, log *slog.Logger) error {
	// 1. Extract: the registry's trading pairs when they are not known yet,
	// then their tickers. Every HTTP request lands in the raw layer, success or
	// failure, so the audit trail matches what went over the wire.
	if p.pairs == nil {
		if err := p.discoverPairs(ctx, log); err != nil {
			return err
		}
	}
	res, err := p.fetcher.FetchTickers(ctx, p.pairs)
	if err != nil {
		p.recordFailure(ctx, log, binance.KindTickers, err, len(p.pairs))
		var ferr *binance.Error
		if errors.As(err, &ferr) && ferr.Code == binance.CodeInvalidSymbol {
			p.pairs = nil
			p.deferDiscovery()
			log.Warn("a requested pair is no longer listed; pairs will be resolved again", "not_before", p.discoveryRetryAt.UTC().Format(time.RFC3339))
		}
		return fmt.Errorf("fetch: %w", err)
	}

	// 2. Raw layer, before any interpretation, so a transform bug never loses data.
	rawID, err := p.recordSuccess(ctx, log, res.Call, len(p.pairs), len(res.Tickers))
	if err != nil {
		return fmt.Errorf("store raw: %w", err)
	}
	p.metrics.LastSuccessTimestamp.WithLabelValues("fetch").Set(float64(res.FetchedAt.Unix()))

	// 3. Transform. Bad records are dropped and reported, never fatal.
	tr := transform.Tickers(res.Tickers, p.cfg.Symbols, p.cfg.AssetIDs, p.cfg.Quote, res.FetchedAt, rawID)
	p.metrics.AssetsRequested.Set(float64(len(p.cfg.Symbols)))
	p.metrics.AssetsPriced.Set(float64(len(tr.Snapshots)))
	p.metrics.AssetsMissing.Set(float64(len(tr.Missing)))
	p.metrics.AssetsStale.Set(float64(len(tr.Stale)))
	p.metrics.TransformErrorsTotal.Add(float64(len(tr.Errors)))
	for _, e := range tr.Errors {
		log.Warn("transformation error", "raw_fetch_id", rawID, "symbol", e.Symbol, "field", e.Field, "reason", e.Reason)
	}
	p.reportCoverage(log, rawID, tr)
	log.Info("transformation completed", "raw_fetch_id", rawID,
		"snapshots", len(tr.Snapshots), "errors", len(tr.Errors), "missing", len(tr.Missing), "unverified", len(tr.Unverified), "stale", len(tr.Stale), "ignored", tr.Ignored)

	// 4. Load. Insert on the stable key; a replayed fetch is a no-op.
	stats, err := p.store.InsertSnapshots(ctx, tr.Snapshots)
	if err != nil {
		log.Error("data save failed", "raw_fetch_id", rawID, "error", err.Error())
		return fmt.Errorf("insert snapshots: %w", err)
	}
	p.metrics.RowsWrittenTotal.WithLabelValues("price_snapshots", "inserted").Add(float64(stats.Inserted))
	p.metrics.RowsWrittenTotal.WithLabelValues("price_snapshots", "skipped").Add(float64(stats.Skipped))
	p.metrics.LastSuccessTimestamp.WithLabelValues("load").Set(float64(p.now().Unix()))
	log.Info("data saved successfully", "raw_fetch_id", rawID, "inserted", stats.Inserted, "skipped", stats.Skipped)

	// 5. Export to the data lake.
	return p.Export(ctx, log)
}

// discoverPairs resolves which registry symbols trade against the quote asset
// and keeps the verified ones. A failure or an empty result is recorded and
// backs the next attempt off, since retrying a permanent condition every poll
// would write a discovery body each time for nothing.
func (p *Pipeline) discoverPairs(ctx context.Context, log *slog.Logger) error {
	if now := p.now(); now.Before(p.discoveryRetryAt) {
		log.Warn("pair discovery backed off after a failure or an empty result", "not_before", p.discoveryRetryAt.UTC().Format(time.RFC3339))
		return fmt.Errorf("pair discovery backed off until %s", p.discoveryRetryAt.UTC().Format(time.RFC3339))
	}
	res, err := p.fetcher.ResolvePairs(ctx)
	if err != nil {
		p.recordFailure(ctx, log, binance.KindPairs, err, len(p.cfg.Symbols))
		p.deferDiscovery()
		return fmt.Errorf("resolve pairs: %w", err)
	}
	if _, err := p.recordSuccess(ctx, log, res.Call, len(p.cfg.Symbols), len(res.Pairs)); err != nil {
		return fmt.Errorf("store raw: %w", err)
	}
	var verified, excluded, unverified []string
	for _, pair := range res.Pairs {
		base := strings.TrimSuffix(pair, p.cfg.Quote)
		if _, ok := p.cfg.AssetIDs[base]; ok {
			verified = append(verified, pair)
		} else if slices.Contains(p.cfg.Excluded, base) {
			excluded = append(excluded, base)
		} else {
			unverified = append(unverified, base)
		}
	}
	p.metrics.SymbolsExcluded.Set(float64(len(excluded)))
	p.metrics.SymbolsUnverified.Set(float64(len(unverified)))
	if len(excluded) > 0 {
		log.Info("registry symbols the source trades under a pair the symbol map excludes; not priced", "count", len(excluded), "symbols", excluded)
	}
	if len(unverified) > 0 {
		log.Warn("registry symbols the source trades that the symbol map has no row for; not priced until reviewed",
			"count", len(unverified), "symbols", unverified)
	}
	if len(verified) == 0 {
		p.deferDiscovery()
		log.Error("no verified registry symbol trades against the quote asset", "quote", p.cfg.Quote, "listed", len(res.Pairs), "not_before", p.discoveryRetryAt.UTC().Format(time.RFC3339))
		return fmt.Errorf("no verified registry symbol trades against %s", p.cfg.Quote)
	}
	p.pairs, p.discoveryBackoff, p.discoveryRetryAt = verified, 0, time.Time{}
	log.Info("trading pairs resolved", "pairs", len(verified), "excluded", len(excluded), "unverified", len(unverified), "symbols", len(p.cfg.Symbols))
	return nil
}

// deferDiscovery doubles the wait before the next discovery attempt, from one
// poll interval up to maxDiscoveryBackoff.
func (p *Pipeline) deferDiscovery() {
	p.discoveryBackoff = min(max(2*p.discoveryBackoff, p.cfg.Interval), maxDiscoveryBackoff)
	p.discoveryRetryAt = p.now().Add(p.discoveryBackoff)
}

// recordSuccess reports one successful HTTP request and writes it to the raw
// layer. The returned row id is the lineage for anything derived from it.
func (p *Pipeline) recordSuccess(ctx context.Context, log *slog.Logger, c binance.Call, requested, returned int) (int64, error) {
	p.observe(c.Kind, "success", c)
	log.Info("api request succeeded", "kind", c.Kind, "status", c.StatusCode, "attempts", c.Attempts,
		"duration_ms", c.Duration.Milliseconds(), "returned", returned, "bytes", len(c.Body))
	id, err := p.store.InsertRawFetch(ctx, postgres.RawFetch{
		FetchedAt: c.FetchedAt, Source: p.cfg.SourceName, Kind: c.Kind, Endpoint: c.Endpoint, HTTPStatus: c.StatusCode,
		Request: c.Request, Response: c.Body, Duration: c.Duration, Attempts: c.Attempts,
		RequestedCount: requested, ReturnedCount: returned, Outcome: "success",
	})
	if err != nil {
		log.Error("raw save failed", "kind", c.Kind, "error", err.Error())
		return 0, err
	}
	p.metrics.RowsWrittenTotal.WithLabelValues("raw_fetches", "inserted").Inc()
	return id, nil
}

// recordFailure reports one failed HTTP request and writes it to the raw layer
// too: one row per request, either way. The body is kept when the source sent
// a JSON error, which is how Binance reports its codes; anything else stays in
// the error text.
func (p *Pipeline) recordFailure(ctx context.Context, log *slog.Logger, kind string, err error, requested int) {
	c := binance.Call{Kind: kind}
	var ferr *binance.Error
	if errors.As(err, &ferr) {
		c = ferr.Call
		log.Error("api request failed", "kind", kind, "status", c.StatusCode, "attempts", c.Attempts,
			"retryable", ferr.Retryable, "code", ferr.Code, "error", ferr.Err.Error())
	} else {
		log.Error("api request failed", "kind", kind, "error", err.Error())
	}
	if c.FetchedAt.IsZero() {
		c.FetchedAt = p.now().UTC()
	}
	if c.Request == nil {
		c.Request = json.RawMessage(`{}`)
	}
	p.observe(kind, "failure", c)
	var body json.RawMessage
	if json.Valid(c.Body) {
		body = c.Body
	}
	if _, ierr := p.store.InsertRawFetch(ctx, postgres.RawFetch{
		FetchedAt: c.FetchedAt, Source: p.cfg.SourceName, Kind: kind, Endpoint: c.Endpoint, HTTPStatus: c.StatusCode,
		Request: c.Request, Response: body, Duration: c.Duration, Attempts: c.Attempts,
		RequestedCount: requested, Outcome: "failure", Error: err.Error(),
	}); ierr != nil {
		log.Error("raw save of failed request failed", "kind", kind, "error", ierr.Error())
	} else {
		p.metrics.RowsWrittenTotal.WithLabelValues("raw_fetches", "inserted").Inc()
	}
}

// observe updates the per-request metrics: outcome, retries and latency, each
// by request kind so pair discovery and ticker fetches are told apart.
func (p *Pipeline) observe(kind, outcome string, c binance.Call) {
	p.metrics.APIRequestsTotal.WithLabelValues(kind, outcome).Inc()
	p.metrics.APIRetriesTotal.WithLabelValues(kind).Add(float64(max(c.Attempts-1, 0)))
	p.metrics.APIRequestDuration.WithLabelValues(kind).Observe(c.Duration.Seconds())
}

// reportCoverage logs the structural gap once, on the first run. Afterwards it
// warns only when a symbol priced last run is missing now, and reports how
// many prices moved, since quiet pairs repeat their last trade between polls.
func (p *Pipeline) reportCoverage(log *slog.Logger, rawID int64, tr transform.Result) {
	priced := make(map[string]string, len(tr.Snapshots))
	for _, s := range tr.Snapshots {
		priced[s.Symbol] = s.Price
	}
	if p.lastPrices == nil {
		if len(tr.Missing) > 0 {
			log.Info("source coverage baseline: registry symbols the source does not quote",
				"raw_fetch_id", rawID, "priced", len(priced), "missing", len(tr.Missing))
		}
	} else {
		dropped, changed, jumped := 0, 0, 0
		for sym, prev := range p.lastPrices {
			cur, ok := priced[sym]
			if !ok {
				dropped++
				continue
			}
			if cur == prev {
				continue
			}
			changed++
			if jumpedBetween(prev, cur) {
				jumped++
				log.Warn("price jump between consecutive polls", "raw_fetch_id", rawID, "symbol", sym, "previous", prev, "current", cur)
			}
		}
		p.metrics.PricesChanged.Set(float64(changed))
		p.metrics.PriceJumpsTotal.Add(float64(jumped))
		if dropped > 0 {
			p.metrics.CoverageDropsTotal.Add(float64(dropped))
			log.Warn("source coverage dropped: symbols priced last run are missing now", "raw_fetch_id", rawID, "dropped", dropped)
		}
		log.Info("price movement since previous run", "raw_fetch_id", rawID, "changed", changed, "unchanged", len(p.lastPrices)-dropped-changed)
	}
	p.lastPrices = priced
}

// restoreBaseline seeds lastPrices from the newest stored snapshots once, so a
// coverage drop that spans a restart still warns instead of re-baselining.
func (p *Pipeline) restoreBaseline(ctx context.Context) {
	if p.lastPrices != nil {
		return
	}
	prev, err := p.store.LatestPrices(ctx)
	switch {
	case err != nil:
		p.log.Warn("coverage baseline restore failed; starting fresh", "error", err.Error())
	case len(prev) > 0:
		p.lastPrices = prev
		p.log.Info("coverage baseline restored from the previous run", "priced", len(prev))
	}
}

// jumpedBetween reports whether the move from prev to cur exceeds
// priceJumpFactor in either direction. Prices are decimal strings; the
// comparison is exact, like every other numeric path here.
func jumpedBetween(prev, cur string) bool {
	a, okA := new(big.Rat).SetString(prev)
	b, okB := new(big.Rat).SetString(cur)
	if !okA || !okB || a.Sign() <= 0 || b.Sign() <= 0 {
		return false
	}
	r := new(big.Rat).Quo(b, a)
	return r.Cmp(big.NewRat(priceJumpFactor, 1)) > 0 || r.Cmp(big.NewRat(1, priceJumpFactor)) < 0
}

// Export appends everything not yet exported to the data lake files. It is
// also callable on its own, for a catch-up after the files were lost.
func (p *Pipeline) Export(ctx context.Context, log *slog.Logger) error {
	if log == nil {
		log = p.log
	}
	for _, step := range []struct {
		name string
		fn   func(context.Context) (export.Result, error)
	}{
		{export.DatasetRaw, p.exporter.ExportRaw},
		{export.DatasetProcessed, p.exporter.ExportProcessed},
	} {
		res, err := step.fn(ctx)
		if err != nil {
			log.Error("export failed", "dataset", step.name, "error", err.Error())
			return fmt.Errorf("export %s: %w", step.name, err)
		}
		p.metrics.ExportRowsTotal.WithLabelValues(step.name).Add(float64(res.Rows))
		if res.Capped {
			p.metrics.ExportCappedTotal.WithLabelValues(step.name).Inc()
			log.Warn("export stopped at its per-run cap; a backlog remains for the next run", "dataset", step.name, "rows", res.Rows)
		}
		log.Info("data exported successfully", "dataset", step.name, "rows", res.Rows, "files", res.Files, "checkpoint", res.LastID)
	}
	p.metrics.LastSuccessTimestamp.WithLabelValues("export").Set(float64(p.now().Unix()))
	return nil
}

// Replay rewrites the snapshots of every stored ticker response with id in
// (afterID, toID], or to the end when toID is 0: each response is transformed
// again and its rows replaced in one transaction, so a transform bug that
// wrote wrong prices is repaired, not only a missed row added. Rewritten rows
// get new surrogate ids and are exported again; readers dedupe on the business
// key. The files cannot express the deletion: a row the rewrite retracted stays
// in them until they are rebuilt. Pair discovery rows and failures carry
// nothing to re-transform.
func (p *Pipeline) Replay(ctx context.Context, afterID, toID int64) error {
	var raws, inserted, deleted, errs int
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows, err := p.store.ListRawFetchesAfter(ctx, afterID, replayChunk)
		if err != nil {
			return fmt.Errorf("replay list after %d: %w", afterID, err)
		}
		if len(rows) == 0 {
			break
		}
		done := false
		for _, row := range rows {
			if toID > 0 && row.ID > toID {
				done = true
				break
			}
			raws++
			if row.Kind != binance.KindTickers || row.Outcome == "failure" {
				continue
			}
			var tickers []binance.Ticker
			if err := json.Unmarshal(row.Response, &tickers); err != nil {
				errs++
				p.log.Warn("replay skipped an undecodable raw response", "raw_fetch_id", row.ID, "error", err.Error())
				continue
			}
			tr := transform.Tickers(tickers, p.cfg.Symbols, p.cfg.AssetIDs, p.cfg.Quote, row.FetchedAt, row.ID)
			errs += len(tr.Errors)
			stats, err := p.store.ReplaceSnapshots(ctx, row.ID, tr.Snapshots)
			if err != nil {
				return fmt.Errorf("replay rewrite for raw_fetch %d: %w", row.ID, err)
			}
			inserted += stats.Inserted
			deleted += stats.Deleted
		}
		if done || len(rows) < replayChunk {
			break
		}
		afterID = rows[len(rows)-1].ID
	}
	p.log.Info("replay completed", "raw_fetches", raws, "deleted", deleted, "inserted", inserted, "errors", errs)
	return nil
}

// Ready implements the readiness check: Postgres answers and the last
// successful run is younger than ReadinessMaxAge. Before the first success the
// service is not ready, which keeps a broken deploy out of rotation.
func (p *Pipeline) Ready(ctx context.Context) (map[string]any, error) {
	detail := map[string]any{"runs": p.runs.Load()}
	if err := p.store.Ping(ctx); err != nil {
		return detail, fmt.Errorf("database: %w", err)
	}
	last := p.lastSuccess.Load()
	if last == 0 {
		return detail, errors.New("no successful run yet")
	}
	t := time.Unix(0, last)
	age := p.now().Sub(t)
	detail["last_success"] = t.UTC().Format(time.RFC3339)
	detail["last_success_age"] = age.Round(time.Second).String()
	if age > p.cfg.ReadinessMaxAge {
		return detail, fmt.Errorf("last success %s ago exceeds %s", age.Round(time.Second), p.cfg.ReadinessMaxAge)
	}
	return detail, nil
}
