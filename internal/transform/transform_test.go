package transform

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"trustwallet-etl/internal/source/binance"
)

var fetchedAt = time.Date(2026, 9, 30, 8, 0, 0, 0, time.UTC)

var ids = map[string]string{"BTC": "c0", "ETH": "c60", "USDC": "c60_t0xusdc", "GOOD": "c60_t0xgood", "LIVE": "c60_t0xlive", "HALTED": "c60_t0xhalted",
	"ZERO": "c1", "NEG": "c2", "TEXT": "c3", "EMPTY": "c4", "NOTRADE": "c5", "MISSINGID": "c6", "SECONDS": "c7", "FUTURE": "c8"}

func ticker(pair, price string, closeTime time.Time, lastID int64) binance.Ticker {
	return binance.Ticker{Symbol: pair, LastPrice: price, CloseTime: closeTime.UnixMilli(), LastID: lastID}
}

func TestTickersFixtureContract(t *testing.T) {
	data, err := os.ReadFile("../source/binance/testdata/ticker_24hr_mini.json")
	if err != nil {
		t.Fatal(err)
	}
	var tickers []binance.Ticker
	if err := json.Unmarshal(data, &tickers); err != nil {
		t.Fatal(err)
	}
	// Transform at the capture's own clock, so the fixture never ages into staleness.
	var capturedAt int64
	byPair := map[string]binance.Ticker{}
	for _, tk := range tickers {
		capturedAt = max(capturedAt, tk.CloseTime)
		byPair[tk.Symbol] = tk
	}
	at := time.UnixMilli(capturedAt).UTC()
	res := Tickers(tickers, []string{"BTC", "ETH", "USDC", "NOT_A_SYMBOL"}, ids, "USDT", at, 42)

	if len(res.Snapshots) != 3 || len(res.Errors) != 0 || len(res.Stale) != 0 {
		t.Fatalf("snapshots=%d errors=%v stale=%v", len(res.Snapshots), res.Errors, res.Stale)
	}
	if !reflect.DeepEqual(res.Missing, []string{"NOT_A_SYMBOL"}) {
		t.Errorf("missing = %v", res.Missing)
	}
	if res.Ignored != len(tickers)-3 {
		t.Errorf("ignored = %d, want %d", res.Ignored, len(tickers)-3)
	}
	btc, src := res.Snapshots[0], byPair["BTCUSDT"]
	if btc.Symbol != "BTC" || btc.AssetID != "c0" || btc.Price != src.LastPrice || btc.QuoteCurrency != "USDT" || btc.RawFetchID != 42 || !btc.FetchedAt.Equal(at) {
		t.Errorf("btc = %+v", btc)
	}
	if !btc.SourceTime.Equal(time.UnixMilli(src.CloseTime)) || btc.SourceTime.Location() != time.UTC || btc.LastTradeID != src.LastID {
		t.Errorf("source clock and trade id must come from the ticker: %+v", btc)
	}
	// Output order follows the wanted list, so exports are deterministic.
	if res.Snapshots[2].Symbol != "USDC" {
		t.Errorf("order = %v", res.Snapshots)
	}
}

// TestTickersNeverLoadsAnUnverifiedSymbol: a symbol the map does not bind to
// an asset is priced by the source but never becomes a row.
func TestTickersNeverLoadsAnUnverifiedSymbol(t *testing.T) {
	tickers := []binance.Ticker{ticker("BTCUSDT", "1", fetchedAt, 1), ticker("GASUSDT", "3", fetchedAt, 1)}
	res := Tickers(tickers, []string{"BTC", "GAS"}, ids, "USDT", fetchedAt, 1)
	if len(res.Snapshots) != 1 || res.Snapshots[0].Symbol != "BTC" || !reflect.DeepEqual(res.Unverified, []string{"GAS"}) || len(res.Missing) != 0 {
		t.Errorf("res = %+v", res)
	}
}

func TestTickersMatchesPairsCaseInsensitively(t *testing.T) {
	res := Tickers([]binance.Ticker{ticker(" btcusdt ", "2", fetchedAt, 1)}, []string{"BTC"}, ids, "usdt", fetchedAt, 1)
	if len(res.Snapshots) != 1 || res.Snapshots[0].Price != "2" || res.Snapshots[0].QuoteCurrency != "USDT" {
		t.Errorf("res = %+v", res)
	}
}

func TestTickersDropsBadRecordsButKeepsGoodOnes(t *testing.T) {
	tickers := []binance.Ticker{
		ticker("GOODUSDT", "4.00000000", fetchedAt, 10),
		ticker("ZEROUSDT", "0.00000000", fetchedAt, 10),
		ticker("NEGUSDT", "-2", fetchedAt, 10),
		ticker("TEXTUSDT", "n/a", fetchedAt, 10),
		ticker("EMPTYUSDT", "", fetchedAt, 10),
		ticker("NOTRADEUSDT", "1", fetchedAt, -1),
		ticker("MISSINGIDUSDT", "1", fetchedAt, 0),                                       // a removed lastId decodes to zero
		{Symbol: "SECONDSUSDT", LastPrice: "1", CloseTime: fetchedAt.Unix(), LastID: 10}, // seconds where milliseconds are expected: 1970
		ticker("FUTUREUSDT", "1", fetchedAt.Add(time.Hour), 10),
	}
	wanted := []string{"GOOD", "ZERO", "NEG", "TEXT", "EMPTY", "NOTRADE", "MISSINGID", "SECONDS", "FUTURE"}
	res := Tickers(tickers, wanted, ids, "USDT", fetchedAt, 1)
	if len(res.Snapshots) != 1 || res.Snapshots[0].Symbol != "GOOD" || res.Snapshots[0].Price != "4.00000000" {
		t.Fatalf("snapshots = %+v", res.Snapshots)
	}
	fields := map[string]int{}
	for _, e := range res.Errors {
		fields[e.Field]++
	}
	if !reflect.DeepEqual(fields, map[string]int{"price": 4, "trade": 2, "closeTime": 2}) {
		t.Errorf("errors = %v", res.Errors)
	}
}

// TestTickersCountsDuplicateSourceRecords: the same pair twice in one response
// keeps the first and reports the second, never silently last-wins.
func TestTickersCountsDuplicateSourceRecords(t *testing.T) {
	tickers := []binance.Ticker{ticker("BTCUSDT", "1", fetchedAt, 1), ticker("BTCUSDT", "2", fetchedAt, 2)}
	res := Tickers(tickers, []string{"BTC"}, ids, "USDT", fetchedAt, 1)
	if len(res.Snapshots) != 1 || res.Snapshots[0].Price != "1" || len(res.Errors) != 1 || res.Errors[0].Field != "symbol" {
		t.Errorf("res = %+v", res)
	}
}

// TestTickersFlagsStalePairsButKeepsThem: a pair halted for a quarter is stale,
// not invalid; it loads flagged on every poll rather than failing on every poll.
func TestTickersFlagsStalePairsButKeepsThem(t *testing.T) {
	tickers := []binance.Ticker{
		ticker("LIVEUSDT", "1", fetchedAt.Add(-time.Minute), 5),
		ticker("HALTEDUSDT", "1", fetchedAt.Add(-90*24*time.Hour), 5),
	}
	res := Tickers(tickers, []string{"HALTED", "LIVE"}, ids, "USDT", fetchedAt, 1)
	if len(res.Snapshots) != 2 || !reflect.DeepEqual(res.Stale, []string{"HALTED"}) {
		t.Errorf("snapshots=%d stale=%v", len(res.Snapshots), res.Stale)
	}
}

func TestTickersIgnoresOtherQuotes(t *testing.T) {
	tickers := []binance.Ticker{
		ticker("BTCUSDT", "83000", fetchedAt, 1),
		ticker("BTCUSDC", "82990", fetchedAt, 1),
		ticker("DOGEUSDT", "0.2", fetchedAt, 1),
	}
	res := Tickers(tickers, []string{"BTC"}, ids, "USDT", fetchedAt, 1)
	if len(res.Snapshots) != 1 || res.Snapshots[0].Price != "83000" || res.Ignored != 2 {
		t.Errorf("res = %+v", res)
	}
}

func TestTickersEmptyResponseReportsAllMissing(t *testing.T) {
	res := Tickers([]binance.Ticker{}, []string{"BTC", "ETH"}, ids, "USDT", fetchedAt, 1)
	if len(res.Snapshots) != 0 || len(res.Missing) != 2 || res.Ignored != 0 {
		t.Errorf("res = %+v", res)
	}
}
