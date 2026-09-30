// Package transform turns a ticker response into price snapshots: one row per
// registry symbol per poll, each bound to the registry asset its symbol was
// verified against. The grain and the clocks are defended in the README's data
// model section.
package transform

import (
	"fmt"
	"math/big"
	"strings"
	"time"

	"trustwallet-etl/internal/source/binance"
)

const (
	// StaleAfter is how old a pair's last source update may be before the
	// pair counts as stale. Binance stops moving closeTime when a pair stops
	// trading.
	StaleAfter = time.Hour
	// maxSourceLead bounds how far ahead of the fetch a source time may sit;
	// beyond it is a clock bug. The lower bound is minSourceTime.
	maxSourceLead = time.Minute
)

// minSourceTime is Binance's first trading day. A source time before it is a
// clock bug, such as seconds sent where milliseconds are expected, which lands
// in 1970. A pair halted for months stays above it: that is stale, and loads
// flagged, rather than a record error on every poll.
var minSourceTime = time.Date(2017, 7, 14, 0, 0, 0, 0, time.UTC)

// Snapshot is the processed, consumer-facing record.
type Snapshot struct {
	RawFetchID    int64     `json:"raw_fetch_id"`
	AssetID       string    `json:"asset_id"`
	Symbol        string    `json:"symbol"`
	QuoteCurrency string    `json:"quote_currency"`
	Price         string    `json:"price"`
	SourceTime    time.Time `json:"source_time"`
	LastTradeID   int64     `json:"last_trade_id"`
	FetchedAt     time.Time `json:"fetched_at"`
}

// RecordError describes one dropped ticker. The batch continues; the error is
// logged and counted so a single bad record cannot stall the pipeline, and so
// the failure is never silent.
type RecordError struct {
	Symbol string
	Field  string
	Reason string
}

func (e RecordError) Error() string {
	return fmt.Sprintf("symbol %q field %s: %s", e.Symbol, e.Field, e.Reason)
}

// Result is the outcome of transforming one response.
type Result struct {
	Snapshots []Snapshot
	Errors    []RecordError
	// Missing are wanted symbols absent from the response: the coverage gap.
	Missing []string
	// Unverified are wanted symbols the response priced but the symbol map does
	// not bind to an asset. They are never loaded.
	Unverified []string
	// Stale are priced symbols whose last source update is older than
	// StaleAfter. They still load; the snapshot records what the source said.
	Stale []string
	// Ignored counts returned tickers that were not wanted.
	Ignored int
}

// Tickers validates and normalises a response, keeping only wanted symbols
// that ids binds to a registry asset. wanted must be upper-cased; a pair's
// name is its base symbol followed by the quote asset. fetchedAt and
// rawFetchID tie every snapshot back to its raw fetch for lineage.
func Tickers(tickers []binance.Ticker, wanted []string, ids map[string]string, quote string, fetchedAt time.Time, rawFetchID int64) Result {
	var res Result
	fetchedAt = fetchedAt.UTC()
	quote = strings.ToUpper(quote)

	byBase := make(map[string]binance.Ticker, len(tickers))
	for _, t := range tickers {
		base, ok := strings.CutSuffix(strings.ToUpper(strings.TrimSpace(t.Symbol)), quote)
		if !ok || base == "" {
			continue
		}
		if _, dup := byBase[base]; dup {
			res.Errors = append(res.Errors, RecordError{Symbol: base, Field: "symbol", Reason: "duplicate ticker in the response; first one kept"})
			continue
		}
		byBase[base] = t
	}
	res.Ignored = len(tickers)

	for _, sym := range wanted {
		t, ok := byBase[sym]
		if !ok {
			res.Missing = append(res.Missing, sym)
			continue
		}
		res.Ignored--
		id, ok := ids[sym]
		if !ok {
			res.Unverified = append(res.Unverified, sym)
			continue
		}
		snap, err := snapshot(t, sym, id, quote, fetchedAt, rawFetchID)
		if err != nil {
			res.Errors = append(res.Errors, *err)
			continue
		}
		if fetchedAt.Sub(snap.SourceTime) > StaleAfter {
			res.Stale = append(res.Stale, sym)
		}
		res.Snapshots = append(res.Snapshots, snap)
	}
	return res
}

// snapshot validates one ticker: the price must be a positive decimal and is
// kept as sent, never through a float; the trade id must be present and
// positive, so a removed field cannot pass as zero; the source time must be
// after the source existed and not ahead of the fetch.
func snapshot(t binance.Ticker, sym, assetID, quote string, fetchedAt time.Time, rawFetchID int64) (Snapshot, *RecordError) {
	price := strings.TrimSpace(t.LastPrice)
	if r, ok := new(big.Rat).SetString(price); !ok || r.Sign() <= 0 {
		return Snapshot{}, &RecordError{Symbol: sym, Field: "price", Reason: fmt.Sprintf("price %q is not a positive decimal", t.LastPrice)}
	}
	if t.LastID <= 0 {
		return Snapshot{}, &RecordError{Symbol: sym, Field: "trade", Reason: fmt.Sprintf("no trade to price from (lastId %d)", t.LastID)}
	}
	sourceTime := time.UnixMilli(t.CloseTime).UTC()
	if sourceTime.Before(minSourceTime) || sourceTime.After(fetchedAt.Add(maxSourceLead)) {
		return Snapshot{}, &RecordError{Symbol: sym, Field: "closeTime", Reason: fmt.Sprintf("source time %s is before the source existed or more than a minute after the fetch", sourceTime.Format(time.RFC3339))}
	}
	return Snapshot{
		RawFetchID:    rawFetchID,
		AssetID:       assetID,
		Symbol:        sym,
		QuoteCurrency: quote,
		Price:         price,
		SourceTime:    sourceTime,
		LastTradeID:   t.LastID,
		FetchedAt:     fetchedAt,
	}, nil
}
