// Package config reads the service configuration from environment variables.
// Every value has a local-development default; the README lists them.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"trustwallet-etl/internal/source/binance"
)

type Config struct {
	PollInterval  time.Duration
	APIURL        string
	APITimeout    time.Duration
	MaxAttempts   int
	Quote         string
	RegistryPath  string
	SymbolMapPath string
	DataDir       string
	LogPath       string
	HTTPAddr      string
	DatabaseURL   string
	// ReadinessMaxAge is how stale the last successful run may be before
	// /readyz fails. Defaults to three poll intervals.
	ReadinessMaxAge time.Duration
	// DrainTimeout is how long a run in flight may keep going after a
	// shutdown signal before it is cut.
	DrainTimeout time.Duration
}

// Environment variable names. Constants so the README and code cannot drift.
const (
	EnvPollInterval    = "ETL_POLL_INTERVAL"
	EnvAPIURL          = "ETL_API_URL"
	EnvAPITimeout      = "ETL_API_TIMEOUT"
	EnvMaxAttempts     = "ETL_MAX_ATTEMPTS"
	EnvQuote           = "ETL_QUOTE_ASSET"
	EnvRegistryPath    = "ETL_REGISTRY_PATH"
	EnvSymbolMapPath   = "ETL_SYMBOL_MAP_PATH"
	EnvDataDir         = "ETL_DATA_DIR"
	EnvLogPath         = "ETL_LOG_PATH"
	EnvHTTPAddr        = "ETL_HTTP_ADDR"
	EnvDatabaseURL     = "DATABASE_URL"
	EnvReadinessMaxAge = "ETL_READINESS_MAX_AGE"
	EnvDrainTimeout    = "ETL_DRAIN_TIMEOUT"
)

const (
	defaultPollInterval = 30 * time.Second
	defaultAPIURL       = binance.DefaultBaseURL
	// 4 s x 2 attempts plus one capped 5 s backoff is 13 s per request; a poll
	// that also resolves pairs makes two, 26 s, inside one 30 s interval.
	defaultAPITimeout   = 4 * time.Second
	defaultMaxAttempts  = 2
	defaultDrainTimeout = 20 * time.Second
	defaultQuote        = "USDT"
	defaultRegistryPath = "reference/trustwallet_assets.csv"
	defaultSymbolMap    = "reference/symbol_map.csv"
	defaultDataDir      = "data"
	defaultLogPath      = "logs/etl.log"
	defaultHTTPAddr     = ":8080"
	defaultDatabaseURL  = "postgres://etl:etl@localhost:5432/etl?sslmode=disable"
)

// FromEnv builds a Config from the process environment, applying defaults and
// validating the result. All problems are reported together.
func FromEnv() (Config, error) {
	var errs []error

	cfg := Config{
		PollInterval:  durationEnv(EnvPollInterval, defaultPollInterval, &errs),
		APIURL:        stringEnv(EnvAPIURL, defaultAPIURL),
		APITimeout:    durationEnv(EnvAPITimeout, defaultAPITimeout, &errs),
		MaxAttempts:   intEnv(EnvMaxAttempts, defaultMaxAttempts, &errs),
		Quote:         strings.ToUpper(stringEnv(EnvQuote, defaultQuote)),
		RegistryPath:  stringEnv(EnvRegistryPath, defaultRegistryPath),
		SymbolMapPath: stringEnv(EnvSymbolMapPath, defaultSymbolMap),
		DataDir:       stringEnv(EnvDataDir, defaultDataDir),
		LogPath:       stringEnv(EnvLogPath, defaultLogPath),
		HTTPAddr:      stringEnv(EnvHTTPAddr, defaultHTTPAddr),
		DatabaseURL:   stringEnv(EnvDatabaseURL, defaultDatabaseURL),
		DrainTimeout:  durationEnv(EnvDrainTimeout, defaultDrainTimeout, &errs),
	}
	cfg.ReadinessMaxAge = durationEnv(EnvReadinessMaxAge, 3*cfg.PollInterval, &errs)

	if cfg.PollInterval <= 0 {
		errs = append(errs, fmt.Errorf("%s must be positive", EnvPollInterval))
	}
	if cfg.APITimeout <= 0 {
		errs = append(errs, fmt.Errorf("%s must be positive", EnvAPITimeout))
	}
	if cfg.MaxAttempts < 1 {
		errs = append(errs, fmt.Errorf("%s must be at least 1", EnvMaxAttempts))
	}
	// Worst case for one poll: a poll that also resolves pairs makes two
	// requests in sequence, each of MaxAttempts timeouts plus the capped sleep
	// before each retry. Keep the whole budget inside the interval so a slow
	// source degrades to overruns, not overlapping runs.
	if cfg.MaxAttempts >= 1 {
		perRequest := cfg.APITimeout*time.Duration(cfg.MaxAttempts) + binance.DefaultMaxBackoff*time.Duration(cfg.MaxAttempts-1)
		if 2*perRequest >= cfg.PollInterval {
			errs = append(errs, fmt.Errorf("%s x %s plus retry backoff, for the two requests of a poll that resolves pairs, must be less than %s", EnvAPITimeout, EnvMaxAttempts, EnvPollInterval))
		}
	}
	if cfg.DrainTimeout <= 0 {
		errs = append(errs, fmt.Errorf("%s must be positive", EnvDrainTimeout))
	}
	if cfg.Quote == "" {
		errs = append(errs, fmt.Errorf("%s must not be empty", EnvQuote))
	}
	return cfg, errors.Join(errs...)
}

func stringEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func durationEnv(key string, def time.Duration, errs *[]error) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return d
}

func intEnv(key string, def int, errs *[]error) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return n
}
