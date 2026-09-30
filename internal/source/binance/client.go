// Package binance fetches Binance spot 24-hour tickers for the registry's pairs
// against one quote asset, with bounded retries. It makes two kinds of HTTP
// request, and each one is reported as a Call so the raw layer can keep one
// row per request. Response shapes and their edge cases are documented in the
// README's data model section.
package binance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"
)

const (
	DefaultBaseURL = "https://api.binance.com"
	// TickerPath is the 24-hour rolling ticker. type=MINI keeps it to the
	// fields this pipeline reads: last price, last update time, last trade id.
	TickerPath = "/api/v3/ticker/24hr"
	// ExchangeInfoPath lists every pair with its status and assets.
	ExchangeInfoPath = "/api/v3/exchangeInfo"
	// DefaultMaxBackoff caps one retry sleep. Exported so config can budget
	// the worst case of a whole poll: attempts x timeout + sleeps, per request.
	DefaultMaxBackoff = 5 * time.Second
	userAgent         = "trustwallet-etl/1.0 (+take-home)"
	// maxBodyBytes bounds how much of a response is read. A ticker response is
	// about 18 KB and the filtered exchangeInfo about 2.5 MB; anything near this
	// limit is a broken upstream, not data.
	maxBodyBytes = 8 << 20
	// maxErrorBodyBytes bounds how much of an error response is kept for the
	// raw layer. Binance error bodies are a few dozen bytes of JSON.
	maxErrorBodyBytes = 4 << 10
	// CodeInvalidSymbol is Binance's error code for an unknown pair in a request.
	CodeInvalidSymbol = -1121
)

// Kinds of HTTP request this client makes. They label raw rows and metrics.
const (
	KindPairs   = "pairs"   // exchangeInfo: which registry symbols trade against the quote
	KindTickers = "tickers" // the 24-hour ticker for those pairs
)

// Request records what was asked, for the raw layer.
type Request struct {
	URL   string            `json:"url"`
	Query map[string]string `json:"query"`
	Quote string            `json:"quote,omitempty"`
	Pairs []string          `json:"pairs,omitempty"`
}

// Call is one HTTP request as the raw layer records it: what was sent, what
// came back, how long the whole thing took including retries, and how many
// attempts it needed. On failure Body holds the error body when the source
// sent one within maxErrorBodyBytes, which is how Binance reports its codes.
type Call struct {
	Kind       string
	Endpoint   string
	Request    json.RawMessage
	StatusCode int
	Body       []byte
	Attempts   int
	Duration   time.Duration
	// FetchedAt is when the final response was received, UTC, or when the
	// last attempt gave up.
	FetchedAt time.Time
}

// Ticker is one pair from the 24-hour ticker. Only the fields the pipeline
// reads are decoded; the raw layer keeps the whole object.
type Ticker struct {
	Symbol    string `json:"symbol"`
	LastPrice string `json:"lastPrice"`
	// CloseTime is when Binance last updated the pair's window, in epoch ms.
	// It stops moving when the pair stops trading.
	CloseTime int64 `json:"closeTime"`
	// LastID is the id of the pair's most recent trade.
	LastID int64 `json:"lastId"`
}

// PairsResult is a successful exchangeInfo call and the registry pairs it
// yielded. Pairs is empty, not an error, when nothing trades against the quote.
type PairsResult struct {
	Call
	Pairs []string
}

// FetchResult is a successful ticker call and its parsed tickers.
type FetchResult struct {
	Call
	Tickers []Ticker
}

// Error is returned when a request fails after retries are exhausted or a
// non-retryable condition is met. It carries the Call so the failure is
// recorded with the same fidelity as a success. Code is Binance's own error
// code from the response body, when there is one.
type Error struct {
	Call
	Code      int
	Retryable bool
	Err       error
}

func (e *Error) Error() string {
	return fmt.Sprintf("binance %s request failed after %d attempt(s), status %d, retryable=%t: %v", e.Kind, e.Attempts, e.StatusCode, e.Retryable, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// Config sets up a Client. Zero values take the defaults in New.
type Config struct {
	BaseURL string
	Quote   string
	// Bases are the upper-cased registry symbols to price.
	Bases          []string
	AttemptTimeout time.Duration
	MaxAttempts    int
	BaseBackoff    time.Duration
	MaxBackoff     time.Duration
	// OnRetry is called before each retry sleep, for logging and metrics.
	OnRetry func(attempt int, wait time.Duration, err error)
	// Sleep replaces the backoff sleep; tests use it to avoid real waiting.
	Sleep func(ctx context.Context, d time.Duration) error
}

// Client makes the two requests. It is not safe for concurrent use; the
// pipeline calls it from its single polling goroutine.
type Client struct {
	cfg  Config
	http *http.Client
	now  func() time.Time
	// pausedUntil honours a Retry-After longer than one poll can wait: no
	// request of either kind goes out before it, which keeps a 429 from
	// becoming a 418 IP ban.
	pausedUntil time.Time
}

// New returns a client, filling defaults for every zero field of cfg.
func New(cfg Config) *Client {
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	if cfg.Quote == "" {
		cfg.Quote = "USDT"
	}
	if cfg.AttemptTimeout <= 0 {
		cfg.AttemptTimeout = 4 * time.Second
	}
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 2
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 500 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = DefaultMaxBackoff
	}
	if cfg.OnRetry == nil {
		cfg.OnRetry = func(int, time.Duration, error) {}
	}
	if cfg.Sleep == nil {
		cfg.Sleep = sleepCtx
	}
	return &Client{cfg: cfg, http: &http.Client{}, now: time.Now}
}

// ResolvePairs asks exchangeInfo which registry symbols have a trading spot
// pair against the quote asset, so a ticker request never names a pair
// Binance does not list. Status is checked again locally, cheaply.
func (c *Client) ResolvePairs(ctx context.Context) (*PairsResult, error) {
	query := url.Values{"permissions": {"SPOT"}, "symbolStatus": {"TRADING"}, "showPermissionSets": {"false"}}
	endpoint := c.cfg.BaseURL + ExchangeInfoPath
	call, ferr := c.get(ctx, KindPairs, endpoint, query, Request{URL: endpoint, Query: flatten(query)})
	if ferr != nil {
		return nil, ferr
	}
	var info struct {
		Symbols []struct {
			Symbol     string `json:"symbol"`
			Status     string `json:"status"`
			BaseAsset  string `json:"baseAsset"`
			QuoteAsset string `json:"quoteAsset"`
		} `json:"symbols"`
	}
	if err := json.Unmarshal(call.Body, &info); err != nil {
		return nil, &Error{Call: call, Err: fmt.Errorf("decode body: %w", err)}
	}
	// The raw layer keeps the four fields this decision rests on for every
	// pair, about 100 KB, not the 2.5 MB of filters and permissions around
	// them that nothing reads. The pairs actually requested are on every
	// ticker row, so nothing this pipeline uses is lost.
	call.Body, _ = json.Marshal(info)
	wanted := make(map[string]bool, len(c.cfg.Bases))
	for _, b := range c.cfg.Bases {
		wanted[b] = true
	}
	pairs := []string{}
	for _, s := range info.Symbols {
		if s.Status == "TRADING" && s.QuoteAsset == c.cfg.Quote && wanted[s.BaseAsset] {
			pairs = append(pairs, s.Symbol)
		}
	}
	sort.Strings(pairs)
	return &PairsResult{Call: call, Pairs: pairs}, nil
}

// FetchTickers requests the 24-hour ticker for the given pairs in one call.
func (c *Client) FetchTickers(ctx context.Context, pairs []string) (*FetchResult, error) {
	symbols, _ := json.Marshal(pairs)
	query := url.Values{"type": {"MINI"}, "symbols": {string(symbols)}}
	endpoint := c.cfg.BaseURL + TickerPath
	call, ferr := c.get(ctx, KindTickers, endpoint, query, Request{URL: endpoint, Query: map[string]string{"type": "MINI"}, Quote: c.cfg.Quote, Pairs: pairs})
	if ferr != nil {
		return nil, ferr
	}
	var tickers []Ticker
	if err := json.Unmarshal(call.Body, &tickers); err != nil || tickers == nil {
		if err == nil {
			err = errors.New("expected a JSON array of tickers")
		}
		return nil, &Error{Call: call, Err: fmt.Errorf("decode body: %w", err)}
	}
	return &FetchResult{Call: call, Tickers: tickers}, nil
}

// get performs one request with bounded retries and returns its Call record
// with a 2xx body, or an Error carrying the same record. Network errors,
// timeouts, 408, 418, 429 and 500, 502, 503, 504 are retried with exponential
// backoff and full jitter, honouring Retry-After; a Retry-After longer than
// MaxBackoff pauses the client until then instead. Everything else fails
// immediately: retrying cannot help.
func (c *Client) get(ctx context.Context, kind, endpoint string, query url.Values, req Request) (Call, *Error) {
	reqRaw, _ := json.Marshal(req)
	call := Call{Kind: kind, Endpoint: endpoint, Request: reqRaw}
	if until := c.pausedUntil; c.now().Before(until) {
		call.FetchedAt = c.now().UTC()
		return call, &Error{Call: call, Retryable: true, Err: fmt.Errorf("paused until %s, as the source asked", until.UTC().Format(time.RFC3339))}
	}
	fullURL := endpoint + "?" + query.Encode()
	start := c.now()
	for attempt := 1; ; attempt++ {
		o := c.attempt(ctx, fullURL)
		call.Attempts, call.Duration, call.StatusCode, call.Body = attempt, c.now().Sub(start), o.status, o.body
		call.FetchedAt = o.fetchedAt
		if call.FetchedAt.IsZero() {
			call.FetchedAt = c.now().UTC()
		}
		if o.err == nil {
			return call, nil
		}
		ferr := &Error{Call: call, Code: o.code, Retryable: o.retryable, Err: o.err}
		if o.retryAfter > c.cfg.MaxBackoff {
			// Longer than one poll may wait: stop, and send nothing until then.
			c.pausedUntil = c.now().Add(o.retryAfter)
			return call, ferr
		}
		if ctx.Err() != nil || !o.retryable || attempt >= c.cfg.MaxAttempts {
			return call, ferr
		}
		wait := c.backoff(attempt, o.retryAfter)
		c.cfg.OnRetry(attempt, wait, ferr)
		if err := c.cfg.Sleep(ctx, wait); err != nil {
			return call, ferr
		}
	}
}

// outcome is one HTTP round trip, classified.
type outcome struct {
	status     int
	body       []byte // the 2xx body, or a bounded error body
	fetchedAt  time.Time
	code       int // Binance error code from an error body, when present
	retryAfter time.Duration
	retryable  bool
	err        error // nil on 2xx
}

// attempt performs one HTTP round trip and classifies the outcome.
func (c *Client) attempt(ctx context.Context, fullURL string) outcome {
	actx, cancel := context.WithTimeout(ctx, c.cfg.AttemptTimeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(actx, http.MethodGet, fullURL, nil)
	if err != nil {
		return outcome{err: fmt.Errorf("build request: %w", err)}
	}
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("User-Agent", userAgent)

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return outcome{retryable: ctx.Err() == nil, err: classifyTransport(err)}
	}
	defer resp.Body.Close()

	fetchedAt := c.now().UTC()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
	if err != nil {
		return outcome{status: resp.StatusCode, fetchedAt: fetchedAt, retryable: true, err: fmt.Errorf("read body: %w", err)}
	}
	if len(data) > maxBodyBytes {
		return outcome{status: resp.StatusCode, fetchedAt: fetchedAt, err: fmt.Errorf("response exceeds %d bytes", maxBodyBytes)}
	}

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return outcome{status: resp.StatusCode, body: data, fetchedAt: fetchedAt}
	// Transient by nature: rate limiting (429, and 418 once Binance has banned
	// the IP for ignoring 429s), request timeout, and the 5xx codes that mean
	// "temporarily broken". 501 and 505 mean the server will never accept this
	// request, so they fall through and fail fast.
	case resp.StatusCode == http.StatusTooManyRequests,
		resp.StatusCode == http.StatusTeapot,
		resp.StatusCode == http.StatusRequestTimeout,
		resp.StatusCode == http.StatusInternalServerError,
		resp.StatusCode == http.StatusBadGateway,
		resp.StatusCode == http.StatusServiceUnavailable,
		resp.StatusCode == http.StatusGatewayTimeout:
		return outcome{
			status: resp.StatusCode, body: errorBody(data), fetchedAt: fetchedAt, retryable: true,
			retryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
			err:        fmt.Errorf("http %d", resp.StatusCode),
		}
	default:
		var apiErr struct {
			Code int `json:"code"`
		}
		_ = json.Unmarshal(data, &apiErr)
		return outcome{
			status: resp.StatusCode, body: errorBody(data), fetchedAt: fetchedAt, code: apiErr.Code,
			err: fmt.Errorf("http %d: %s", resp.StatusCode, truncate(data, 200)),
		}
	}
}

// backoff computes the wait before the next attempt: exponential with full
// jitter, capped, and never shorter than an explicit Retry-After.
func (c *Client) backoff(attempt int, retryAfter time.Duration) time.Duration {
	exp := c.cfg.BaseBackoff << (attempt - 1)
	if exp > c.cfg.MaxBackoff || exp <= 0 {
		exp = c.cfg.MaxBackoff
	}
	wait := time.Duration(rand.Int64N(int64(exp) + 1))
	if retryAfter > wait {
		wait = retryAfter // at most MaxBackoff: longer ones pause the client in get
	}
	return wait
}

// parseRetryAfter reads the delay-seconds form of the header, which is the
// only form this source sends. Anything else counts as absent.
func parseRetryAfter(v string) time.Duration {
	if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	return 0
}

// errorBody keeps a bounded copy of an error response for the raw layer.
func errorBody(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	if len(data) > maxErrorBodyBytes {
		data = data[:maxErrorBodyBytes]
	}
	return append([]byte(nil), data...)
}

func classifyTransport(err error) error {
	var ne net.Error
	if (errors.As(err, &ne) && ne.Timeout()) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("timeout: %w", err)
	}
	return fmt.Errorf("transport: %w", err)
}

func flatten(q url.Values) map[string]string {
	out := make(map[string]string, len(q))
	for k := range q {
		out[k] = q.Get(k)
	}
	return out
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
