// Command etl polls Binance's public 24-hour ticker for the registry's pairs
// every 30 seconds, stores raw responses and normalised price snapshots in
// Postgres, and appends both to JSON Lines files under data/. It serves
// /healthz, /readyz and /metrics for monitoring.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"trustwallet-etl/internal/config"
	"trustwallet-etl/internal/export"
	"trustwallet-etl/internal/observe"
	"trustwallet-etl/internal/pipeline"
	"trustwallet-etl/internal/registry"
	"trustwallet-etl/internal/source/binance"
	"trustwallet-etl/internal/store/postgres"
	"trustwallet-etl/migrations"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	once := flag.Bool("once", false, "run a single cycle and exit")
	exportOnly := flag.Bool("export-only", false, "export unexported rows to the data directory and exit, without polling")
	replayAfter := flag.Int64("replay-after", -1, "rewrite the snapshots of stored ticker responses with id greater than this, export, then exit")
	replayTo := flag.Int64("replay-to", 0, "with -replay-after: the last raw fetch id to rewrite, inclusive; 0 means to the end")
	flag.Parse()

	if err := run(*once, *exportOnly, *replayAfter, *replayTo); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run(once, exportOnly bool, replayAfter, replayTo int64) error {
	cfg, err := config.FromEnv()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	log, closeLog, err := observe.NewLogger(cfg.LogPath, slog.LevelInfo)
	if err != nil {
		return err
	}
	defer closeLog.Close()
	log = log.With("service", "trustwallet-etl", "version", version)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	assets, err := registry.Load(cfg.RegistryPath)
	if err != nil {
		log.Error("registry load failed", "path", cfg.RegistryPath, "error", err.Error())
		return err
	}
	metrics := observe.NewMetrics()
	if amb := registry.AmbiguousSymbols(assets); len(amb) > 0 {
		metrics.RegistryAmbiguous.Set(float64(len(amb)))
		log.Warn("registry has symbols shared by several assets; only the symbol map decides which one a price belongs to", "symbols", amb)
	}
	symbolMap, err := registry.LoadSymbolMap(cfg.SymbolMapPath, assets)
	if err != nil {
		log.Error("symbol map load failed", "path", cfg.SymbolMapPath, "error", err.Error())
		return err
	}
	log.Info("symbol map loaded", "verified", len(symbolMap.Verified), "excluded", len(symbolMap.Excluded), "excluded_symbols", symbolMap.Excluded, "path", cfg.SymbolMapPath)

	store, err := postgres.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("database connection failed", "error", err.Error())
		return err
	}
	defer store.Close()
	applied, err := store.Migrate(ctx, migrations.FS)
	if err != nil {
		log.Error("migration failed", "error", err.Error())
		return err
	}
	release, err := store.AcquireWriterLock(ctx)
	if err != nil {
		log.Error("cannot start: another writer is active", "error", err.Error())
		return err
	}
	defer release()
	loaded, removed, err := store.UpsertAssets(ctx, assets)
	if err != nil {
		log.Error("registry load into database failed", "error", err.Error())
		return err
	}
	log.Info("database ready", "migrations_applied", applied, "assets_loaded", loaded, "assets_removed", removed, "registry", cfg.RegistryPath)

	symbols := registry.Symbols(assets)
	client := binance.New(binance.Config{
		BaseURL: cfg.APIURL, Quote: cfg.Quote, Bases: symbols, AttemptTimeout: cfg.APITimeout, MaxAttempts: cfg.MaxAttempts,
		OnRetry: func(attempt int, wait time.Duration, err error) {
			var ferr *binance.Error
			kind := "unknown"
			if errors.As(err, &ferr) {
				kind = ferr.Kind
			}
			log.Warn("api request retrying", "kind", kind, "attempt", attempt, "wait", wait.String(), "error", err.Error())
		},
	})
	exporter := export.New(store, cfg.DataDir)
	p := pipeline.New(pipeline.Config{
		Interval:        cfg.PollInterval,
		ReadinessMaxAge: cfg.ReadinessMaxAge,
		DrainTimeout:    cfg.DrainTimeout,
		SourceName:      "binance",
		Quote:           cfg.Quote,
		Symbols:         symbols,
		AssetIDs:        symbolMap.Verified,
		Excluded:        symbolMap.Excluded,
	}, client, store, exporter, log, metrics)

	if replayAfter >= 0 {
		if err := p.Replay(ctx, replayAfter, replayTo); err != nil {
			return err
		}
		return p.Export(ctx, nil)
	}
	if exportOnly {
		return p.Export(ctx, nil)
	}
	if once {
		return p.RunOnce(ctx)
	}

	srv := observe.NewServer(cfg.HTTPAddr, metrics.Registry, p.Ready)
	serverErr := make(chan error, 1)
	go func() {
		log.Info("http server listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	runErr := make(chan error, 1)
	go func() { runErr <- p.Run(ctx) }()

	select {
	case err := <-serverErr:
		log.Error("http server failed", "error", err.Error())
		stop()
		<-runErr
		return err
	case err := <-runErr:
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		if errors.Is(err, context.Canceled) {
			log.Info("shutdown complete")
			return nil
		}
		return err
	}
}
