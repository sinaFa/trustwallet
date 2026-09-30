package binance

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// infoBody lists pairs the resolution must reject for three different reasons
// next to the two it must keep.
const infoBody = `{"timezone":"UTC","symbols":[
{"symbol":"BTCUSDT","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT"},
{"symbol":"ETHUSDT","status":"TRADING","baseAsset":"ETH","quoteAsset":"USDT"},
{"symbol":"SOLUSDT","status":"BREAK","baseAsset":"SOL","quoteAsset":"USDT"},
{"symbol":"BTCUSDC","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDC"},
{"symbol":"DOGEUSDT","status":"TRADING","baseAsset":"DOGE","quoteAsset":"USDT"}]}`

const okBody = `[{"symbol":"BTCUSDT","lastPrice":"83286.02000000","closeTime":1790753370001,"lastId":6724035014},` +
	`{"symbol":"ETHUSDT","lastPrice":"2670.28000000","closeTime":1790753369000,"lastId":2946021100}]`

var pairs = []string{"BTCUSDT", "ETHUSDT"}

func noSleep(context.Context, time.Duration) error { return nil }

// fakeBinance serves exchangeInfo from info and routes ticker requests to h.
func fakeBinance(t *testing.T, info string, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v3/exchangeInfo", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(info)) })
	mux.HandleFunc("GET /api/v3/ticker/24hr", h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func newTestClient(srv *httptest.Server, override func(*Config)) *Client {
	cfg := Config{BaseURL: srv.URL, Quote: "USDT", Bases: []string{"BTC", "ETH", "SOL"}, Sleep: noSleep, MaxAttempts: 3,
		BaseBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond, AttemptTimeout: 2 * time.Second}
	if override != nil {
		override(&cfg)
	}
	return New(cfg)
}

func TestResolvePairsFiltersAndRecordsTheCall(t *testing.T) {
	var gotQuery string
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v3/exchangeInfo", func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		_, _ = w.Write([]byte(infoBody))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	res, err := newTestClient(srv, nil).ResolvePairs(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// SOLUSDT is not trading, BTCUSDC is another quote, DOGE is not in the registry.
	if !reflect.DeepEqual(res.Pairs, pairs) {
		t.Errorf("pairs = %v", res.Pairs)
	}
	if !strings.Contains(gotQuery, "symbolStatus=TRADING") || !strings.Contains(gotQuery, "permissions=SPOT") {
		t.Errorf("query = %s", gotQuery)
	}
	if res.Kind != KindPairs || res.Endpoint != srv.URL+ExchangeInfoPath || res.StatusCode != 200 || res.Attempts != 1 {
		t.Errorf("call = %+v", res.Call)
	}
	// The recorded body is trimmed to the fields the decision rests on, for every pair.
	var kept struct {
		Symbols []map[string]string `json:"symbols"`
	}
	if err := json.Unmarshal(res.Body, &kept); err != nil || len(kept.Symbols) != 5 || kept.Symbols[2]["status"] != "BREAK" || len(kept.Symbols[0]) != 4 {
		t.Errorf("trimmed body = %s err=%v", res.Body, err)
	}
	var req Request
	if err := json.Unmarshal(res.Request, &req); err != nil || req.Query["symbolStatus"] != "TRADING" || req.URL != res.Endpoint {
		t.Errorf("request record = %s err=%v", res.Request, err)
	}
	if res.FetchedAt.Location() != time.UTC || res.Duration <= 0 {
		t.Errorf("FetchedAt must be UTC and Duration measured: %+v", res.Call)
	}
}

// TestResolvePairsWithNoMatchIsNotAnError: an empty result is a fact about the
// source, recorded as a successful call; the pipeline decides what it means.
func TestResolvePairsWithNoMatchIsNotAnError(t *testing.T) {
	srv := fakeBinance(t, infoBody, func(w http.ResponseWriter, r *http.Request) {})
	res, err := newTestClient(srv, func(c *Config) { c.Bases = []string{"NOPE"} }).ResolvePairs(context.Background())
	if err != nil || len(res.Pairs) != 0 || res.StatusCode != 200 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}

func TestFetchTickersSendsExpectedRequest(t *testing.T) {
	var gotType, gotSymbols, gotAccept string
	srv := fakeBinance(t, infoBody, func(w http.ResponseWriter, r *http.Request) {
		gotType, gotSymbols, gotAccept = r.URL.Query().Get("type"), r.URL.Query().Get("symbols"), r.Header.Get("Accept")
		_, _ = w.Write([]byte(okBody))
	})

	res, err := newTestClient(srv, nil).FetchTickers(context.Background(), pairs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotType != "MINI" || gotSymbols != `["BTCUSDT","ETHUSDT"]` || gotAccept != "application/json" {
		t.Errorf("type=%s symbols=%s accept=%s", gotType, gotSymbols, gotAccept)
	}
	if res.Kind != KindTickers || res.Endpoint != srv.URL+TickerPath || res.Attempts != 1 || res.StatusCode != 200 || len(res.Tickers) != 2 {
		t.Errorf("result = %+v", res)
	}
	if tk := res.Tickers[0]; tk.LastPrice != "83286.02000000" || tk.CloseTime != 1790753370001 || tk.LastID != 6724035014 {
		t.Errorf("ticker fields must decode verbatim: %+v", tk)
	}
	var req Request
	if err := json.Unmarshal(res.Request, &req); err != nil || !reflect.DeepEqual(req.Pairs, pairs) || req.Quote != "USDT" {
		t.Errorf("request record = %s err=%v", res.Request, err)
	}
	if string(res.Body) != okBody || res.FetchedAt.Location() != time.UTC {
		t.Errorf("raw bytes must be kept for the raw layer: %+v", res.Call)
	}
}

func TestFetchTickersRetriesOn5xxThenSucceeds(t *testing.T) {
	var calls atomic.Int32
	srv := fakeBinance(t, infoBody, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte(okBody))
	})

	var retries int
	c := newTestClient(srv, func(c *Config) { c.OnRetry = func(int, time.Duration, error) { retries++ } })
	res, err := c.FetchTickers(context.Background(), pairs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Attempts != 3 || retries != 2 || calls.Load() != 3 {
		t.Errorf("attempts=%d retries=%d calls=%d", res.Attempts, retries, calls.Load())
	}
}

// TestFetchTickersGivesUpAndReportsTheCall: an exhausted request still yields
// a full Call record, so the failure lands in the raw layer with the same
// fidelity as a success.
func TestFetchTickersGivesUpAndReportsTheCall(t *testing.T) {
	var calls atomic.Int32
	srv := fakeBinance(t, infoBody, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"code":-1001,"msg":"Internal error."}`))
	})

	_, err := newTestClient(srv, nil).FetchTickers(context.Background(), pairs)
	var ferr *Error
	if !errors.As(err, &ferr) {
		t.Fatalf("expected *Error, got %T %v", err, err)
	}
	if ferr.Attempts != 3 || ferr.StatusCode != 503 || !ferr.Retryable || calls.Load() != 3 {
		t.Errorf("err = %+v calls=%d", ferr, calls.Load())
	}
	if ferr.Kind != KindTickers || ferr.Endpoint != srv.URL+TickerPath || ferr.Duration <= 0 || ferr.FetchedAt.IsZero() || len(ferr.Request) == 0 {
		t.Errorf("call record on failure = %+v", ferr.Call)
	}
	if string(ferr.Body) != `{"code":-1001,"msg":"Internal error."}` {
		t.Errorf("error body must be kept: %q", ferr.Body)
	}
}

func TestFetchTickersDoesNotRetry4xxAndKeepsTheCode(t *testing.T) {
	var calls atomic.Int32
	srv := fakeBinance(t, infoBody, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":-1121,"msg":"Invalid symbol."}`))
	})

	_, err := newTestClient(srv, nil).FetchTickers(context.Background(), pairs)
	var ferr *Error
	if !errors.As(err, &ferr) || ferr.Retryable || ferr.StatusCode != 400 || ferr.Code != CodeInvalidSymbol || calls.Load() != 1 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
	if !json.Valid(ferr.Body) {
		t.Errorf("the JSON error body must be kept for the raw layer: %q", ferr.Body)
	}
}

// TestErrorBodyIsBounded: a huge error page is kept only up to the bound, so
// the raw layer never stores an upstream's HTML dump.
func TestErrorBodyIsBounded(t *testing.T) {
	srv := fakeBinance(t, infoBody, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(make([]byte, maxErrorBodyBytes*3))
	})
	_, err := newTestClient(srv, nil).FetchTickers(context.Background(), pairs)
	var ferr *Error
	if !errors.As(err, &ferr) || len(ferr.Body) != maxErrorBodyBytes {
		t.Fatalf("err=%v body=%d bytes", err, len(ferr.Body))
	}
}

// TestFetchTickers501IsNotRetried pins the 5xx split: 500, 502, 503 and 504 are
// transient, while 501 means the server will never accept this request, so a
// retry is noise.
func TestFetchTickers501IsNotRetried(t *testing.T) {
	var calls atomic.Int32
	srv := fakeBinance(t, infoBody, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNotImplemented)
	})
	_, err := newTestClient(srv, nil).FetchTickers(context.Background(), pairs)
	var ferr *Error
	if !errors.As(err, &ferr) || ferr.Retryable || ferr.StatusCode != 501 || calls.Load() != 1 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
}

// TestFetchTickersToleratesUnknownFields pins that an additive upstream change,
// new fields on a ticker, parses cleanly and keeps the known fields intact.
func TestFetchTickersToleratesUnknownFields(t *testing.T) {
	body := `[{"symbol":"BTCUSDT","lastPrice":"1.5","closeTime":1,"lastId":2,"vwap":{"1h":"1.4"}}]`
	srv := fakeBinance(t, infoBody, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) })
	res, err := newTestClient(srv, nil).FetchTickers(context.Background(), pairs)
	if err != nil {
		t.Fatalf("an additive change must not break parsing: %v", err)
	}
	if res.Tickers[0].LastPrice != "1.5" || string(res.Body) != body {
		t.Errorf("res = %+v", res)
	}
}

func TestFetchTickersHonoursRetryAfter(t *testing.T) {
	var calls atomic.Int32
	srv := fakeBinance(t, infoBody, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(okBody))
	})
	var waited time.Duration
	c := newTestClient(srv, func(c *Config) {
		c.MaxBackoff = 10 * time.Second
		c.OnRetry = func(_ int, wait time.Duration, _ error) { waited = wait }
	})
	if _, err := c.FetchTickers(context.Background(), pairs); err != nil {
		t.Fatal(err)
	}
	if waited != 2*time.Second {
		t.Errorf("waited %s, want the 2s the server asked for", waited)
	}
}

// TestPauseWhenRetryAfterExceedsBudget: a Retry-After longer than one poll may
// wait stops retrying at once, and no request of either kind goes out until it
// expires. Hammering through a 429 is how Binance escalates to a 418 IP ban,
// and a 418 carries the ban's length in the same header.
func TestPauseWhenRetryAfterExceedsBudget(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusTeapot} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var calls atomic.Int32
			srv := fakeBinance(t, infoBody, func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					w.Header().Set("Retry-After", "120")
					w.WriteHeader(status)
					return
				}
				_, _ = w.Write([]byte(okBody))
			})
			var retries int
			c := newTestClient(srv, func(c *Config) { c.OnRetry = func(int, time.Duration, error) { retries++ } })
			clock := time.Date(2026, 9, 30, 8, 0, 0, 0, time.UTC)
			c.now = func() time.Time { return clock }

			if _, err := c.FetchTickers(context.Background(), pairs); err == nil || retries != 0 || calls.Load() != 1 {
				t.Fatalf("first poll: err=%v retries=%d calls=%d", err, retries, calls.Load())
			}
			clock = clock.Add(time.Minute)
			if _, err := c.ResolvePairs(context.Background()); err == nil || !strings.Contains(err.Error(), "paused until") {
				t.Fatalf("the pause must cover both request kinds: err=%v", err)
			}
			if _, err := c.FetchTickers(context.Background(), pairs); err == nil || calls.Load() != 1 {
				t.Fatalf("while paused: err=%v calls=%d", err, calls.Load())
			}
			clock = clock.Add(2 * time.Minute)
			if _, err := c.FetchTickers(context.Background(), pairs); err != nil || calls.Load() != 2 {
				t.Fatalf("after the pause: err=%v calls=%d", err, calls.Load())
			}
		})
	}
}

func TestFetchTickersMalformedBodyIsNotRetried(t *testing.T) {
	for name, body := range map[string]string{
		"empty body":      ``,
		"truncated json":  `[{"symbol":"BTCUSDT","lastPr`,
		"null":            `null`,
		"object":          `{"code":0,"msg":"unexpected"}`,
		"html error page": `<html><body>502 Bad Gateway</body></html>`,
	} {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			srv := fakeBinance(t, infoBody, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_, _ = w.Write([]byte(body))
			})
			_, err := newTestClient(srv, nil).FetchTickers(context.Background(), pairs)
			var ferr *Error
			if !errors.As(err, &ferr) || ferr.Retryable || calls.Load() != 1 {
				t.Fatalf("err=%v calls=%d", err, calls.Load())
			}
		})
	}
}

// TestFetchTickersEmptyArrayIsASuccessfulFetch pins the boundary: a well-formed
// response with no tickers is not a client error. Coverage is the transform's job.
func TestFetchTickersEmptyArrayIsASuccessfulFetch(t *testing.T) {
	srv := fakeBinance(t, infoBody, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`[]`)) })
	res, err := newTestClient(srv, nil).FetchTickers(context.Background(), pairs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Tickers == nil || len(res.Tickers) != 0 {
		t.Errorf("tickers = %v", res.Tickers)
	}
}

func TestFetchTickersTimeoutIsRetried(t *testing.T) {
	var calls atomic.Int32
	srv := fakeBinance(t, infoBody, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			time.Sleep(300 * time.Millisecond)
		}
		_, _ = w.Write([]byte(okBody))
	})
	c := newTestClient(srv, func(c *Config) { c.AttemptTimeout = 50 * time.Millisecond })
	res, err := c.FetchTickers(context.Background(), pairs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Attempts != 2 {
		t.Errorf("expected 2 attempts, got %d", res.Attempts)
	}
}

func TestFetchTickersStopsWhenContextCancelled(t *testing.T) {
	var calls atomic.Int32
	srv := fakeBinance(t, infoBody, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})

	ctx, cancel := context.WithCancel(context.Background())
	c := newTestClient(srv, func(c *Config) { c.OnRetry = func(int, time.Duration, error) { cancel() } })
	if _, err := c.FetchTickers(ctx, pairs); err == nil {
		t.Fatal("expected error")
	}
	if calls.Load() != 1 {
		t.Errorf("expected no attempt after cancel, got %d calls", calls.Load())
	}
}

func TestFetchTickersRejectsOversizedBody(t *testing.T) {
	srv := fakeBinance(t, infoBody, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(make([]byte, maxBodyBytes+10))
	})
	_, err := newTestClient(srv, nil).FetchTickers(context.Background(), pairs)
	var ferr *Error
	if !errors.As(err, &ferr) || ferr.Retryable {
		t.Fatalf("expected non-retryable error, got %v", err)
	}
}

// TestContractFixturesParse pins both response shapes against real captures
// from 2026-09-30: the 24-hour ticker for the registry's 62 USDT pairs, and
// exchangeInfo trimmed to the fields the client reads. If Binance changes
// either shape, this is the test that should fail.
func TestContractFixturesParse(t *testing.T) {
	data, err := os.ReadFile("testdata/ticker_24hr_mini.json")
	if err != nil {
		t.Fatal(err)
	}
	var tickers []Ticker
	if err := json.Unmarshal(data, &tickers); err != nil {
		t.Fatalf("ticker fixture no longer decodes: %v", err)
	}
	if len(tickers) != 62 {
		t.Fatalf("ticker fixture: %d pairs", len(tickers))
	}
	for _, tk := range tickers {
		if tk.Symbol == "" || tk.LastPrice == "" || tk.CloseTime <= 0 || tk.LastID <= 0 {
			t.Errorf("incomplete ticker in fixture: %+v", tk)
		}
	}

	info, err := os.ReadFile("testdata/exchange_info.json")
	if err != nil {
		t.Fatal(err)
	}
	srv := fakeBinance(t, string(info), func(w http.ResponseWriter, r *http.Request) {})
	c := newTestClient(srv, func(c *Config) { c.Bases = []string{"BTC", "ETH", "BNB", "SOL", "USDC"} })
	res, err := c.ResolvePairs(context.Background())
	if err != nil {
		t.Fatalf("exchangeInfo fixture no longer resolves: %v", err)
	}
	if want := []string{"BNBUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT", "USDCUSDT"}; !reflect.DeepEqual(res.Pairs, want) {
		t.Errorf("pairs = %v, want %v", res.Pairs, want)
	}
}

func TestParseRetryAfter(t *testing.T) {
	for in, want := range map[string]time.Duration{"": 0, "5": 5 * time.Second, "-1": 0, "garbage": 0} {
		if got := parseRetryAfter(in); got != want {
			t.Errorf("parseRetryAfter(%q) = %s, want %s", in, got, want)
		}
	}
}
