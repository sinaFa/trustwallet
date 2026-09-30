package config

import (
	"strings"
	"testing"
	"time"
)

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{EnvPollInterval, EnvAPIURL, EnvAPITimeout, EnvMaxAttempts, EnvQuote, EnvRegistryPath, EnvSymbolMapPath,
		EnvDataDir, EnvLogPath, EnvHTTPAddr, EnvDatabaseURL, EnvReadinessMaxAge, EnvDrainTimeout} {
		t.Setenv(k, "")
	}
}

func TestDefaultsAreValid(t *testing.T) {
	clearEnv(t)
	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("defaults must validate: %v", err)
	}
	if cfg.PollInterval != 30*time.Second || cfg.Quote != "USDT" || cfg.ReadinessMaxAge != 90*time.Second || cfg.DrainTimeout != 20*time.Second {
		t.Errorf("cfg = %+v", cfg)
	}
	// Two requests of MaxAttempts timeouts plus the capped sleeps fit one interval.
	if worst := 2 * (cfg.APITimeout*time.Duration(cfg.MaxAttempts) + 5*time.Second*time.Duration(cfg.MaxAttempts-1)); worst >= cfg.PollInterval {
		t.Errorf("default retries could overrun the interval: worst case %s", worst)
	}
}

func TestDrainTimeoutMustBePositive(t *testing.T) {
	clearEnv(t)
	t.Setenv(EnvDrainTimeout, "0s")
	if _, err := FromEnv(); err == nil || !strings.Contains(err.Error(), EnvDrainTimeout) {
		t.Errorf("err = %v", err)
	}
}

func TestOverridesAndReadinessDerivedFromInterval(t *testing.T) {
	clearEnv(t)
	t.Setenv(EnvPollInterval, "60s")
	t.Setenv(EnvAPITimeout, "2s")
	t.Setenv(EnvMaxAttempts, "2")
	t.Setenv(EnvQuote, "usdc")
	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	// Pair names are upper-case, so the quote asset is normalised.
	if cfg.PollInterval != 60*time.Second || cfg.ReadinessMaxAge != 180*time.Second || cfg.MaxAttempts != 2 || cfg.Quote != "USDC" {
		t.Errorf("cfg = %+v", cfg)
	}
}

func TestInvalidValuesAreReportedTogether(t *testing.T) {
	clearEnv(t)
	t.Setenv(EnvPollInterval, "soon")
	t.Setenv(EnvMaxAttempts, "0")
	t.Setenv(EnvAPITimeout, "-1s")
	_, err := FromEnv()
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{EnvPollInterval, EnvMaxAttempts, EnvAPITimeout} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %s: %v", want, err)
		}
	}
}

func TestRetriesMustFitInsideInterval(t *testing.T) {
	clearEnv(t)
	t.Setenv(EnvPollInterval, "30s")
	t.Setenv(EnvAPITimeout, "10s")
	t.Setenv(EnvMaxAttempts, "3")
	if _, err := FromEnv(); err == nil || !strings.Contains(err.Error(), "must be less than") {
		t.Errorf("10s x 3 must not fit in 30s, got %v", err)
	}

	// The budget covers a whole poll, which can make two requests: 5s x 2 plus
	// one 5s backoff is 15s per request, which fits alone but not twice.
	t.Setenv(EnvAPITimeout, "5s")
	t.Setenv(EnvMaxAttempts, "2")
	if _, err := FromEnv(); err == nil || !strings.Contains(err.Error(), "two requests") {
		t.Errorf("15s per request must not fit twice in 30s, got %v", err)
	}
	// 4s x 2 plus one 5s backoff is 13s per request, 26s for two: fits.
	t.Setenv(EnvAPITimeout, "4s")
	if _, err := FromEnv(); err != nil {
		t.Errorf("13s per request must fit twice in 30s, got %v", err)
	}
}
