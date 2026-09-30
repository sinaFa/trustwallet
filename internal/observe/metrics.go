package observe

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics are the pipeline's Prometheus instruments. Each answers one
// operational question no other metric answers; the README says which.
type Metrics struct {
	Registry *prometheus.Registry

	APIRequestsTotal   *prometheus.CounterVec   // kind=pairs|tickers, outcome=success|failure
	APIRetriesTotal    *prometheus.CounterVec   // kind
	APIRequestDuration *prometheus.HistogramVec // kind

	AssetsRequested      prometheus.Gauge
	AssetsPriced         prometheus.Gauge
	AssetsMissing        prometheus.Gauge
	AssetsStale          prometheus.Gauge   // priced symbols with no source update for over an hour
	CoverageDropsTotal   prometheus.Counter // symbols priced last run but not this run
	PricesChanged        prometheus.Gauge   // symbols whose price differs from the previous run
	PriceJumpsTotal      prometheus.Counter // prices that moved implausibly far between two polls
	RegistryAmbiguous    prometheus.Gauge   // symbols shared by several registry assets
	SymbolsExcluded      prometheus.Gauge   // symbols the source trades that the symbol map reviewed and excluded
	SymbolsUnverified    prometheus.Gauge   // symbols the source trades that the symbol map has no row for
	TransformErrorsTotal prometheus.Counter
	RowsWrittenTotal     *prometheus.CounterVec // table, op
	ExportRowsTotal      *prometheus.CounterVec // dataset
	ExportCappedTotal    *prometheus.CounterVec // dataset: runs that stopped at the per-run cap with a backlog left

	RunsTotal            *prometheus.CounterVec // outcome=success|failure
	RunOverrunsTotal     prometheus.Counter     // runs longer than the poll interval
	LastSuccessTimestamp *prometheus.GaugeVec   // stage=fetch|load|export
}

// NewMetrics registers all instruments on a fresh registry, together with the
// default Go runtime and process collectors.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		Registry: reg,
		APIRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_api_requests_total", Help: "Source API requests by kind (pairs, tickers) and outcome.",
		}, []string{"kind", "outcome"}),
		APIRetriesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_api_retries_total", Help: "Retried HTTP attempts against the source API, by request kind.",
		}, []string{"kind"}),
		APIRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "etl_api_request_duration_seconds", Help: "Wall time of one source request including its retries, by kind.",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		}, []string{"kind"}),
		AssetsRequested: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "etl_assets_requested", Help: "Registry symbols the pipeline wants priced.",
		}),
		AssetsPriced: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "etl_assets_priced", Help: "Registry symbols priced in the last poll.",
		}),
		AssetsMissing: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "etl_assets_missing", Help: "Registry symbols absent from the last response.",
		}),
		AssetsStale: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "etl_assets_stale", Help: "Priced symbols whose last source update is more than an hour old.",
		}),
		CoverageDropsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "etl_coverage_drops_total", Help: "Symbols that were priced in the previous run and are missing in this one.",
		}),
		PricesChanged: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "etl_prices_changed_last_run", Help: "Symbols whose price differs from the previous run. Zero for many runs means the source feed is stale.",
		}),
		PriceJumpsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "etl_price_jumps_total", Help: "Prices that moved more than 10x between consecutive polls. Flagged and loaded, never dropped.",
		}),
		RegistryAmbiguous: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "etl_registry_ambiguous_symbols", Help: "Registry symbols shared by more than one asset.",
		}),
		SymbolsExcluded: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "etl_symbols_excluded", Help: "Registry symbols the source trades under a pair reference/symbol_map.csv reviewed and excluded as a different project. Never priced.",
		}),
		SymbolsUnverified: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "etl_symbols_unverified", Help: "Registry symbols the source trades that reference/symbol_map.csv has no row for. Never priced until reviewed.",
		}),
		TransformErrorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "etl_transform_errors_total", Help: "Tickers dropped by validation.",
		}),
		RowsWrittenTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_rows_written_total", Help: "Rows written to Postgres by table and operation.",
		}, []string{"table", "op"}),
		ExportRowsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_export_rows_total", Help: "Rows appended to JSONL files by dataset.",
		}, []string{"dataset"}),
		ExportCappedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_export_capped_total", Help: "Export runs that stopped at the per-run row cap with a backlog left for the next run.",
		}, []string{"dataset"}),
		RunsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_runs_total", Help: "Pipeline runs by outcome.",
		}, []string{"outcome"}),
		RunOverrunsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "etl_run_overruns_total", Help: "Runs that took longer than the poll interval.",
		}),
		LastSuccessTimestamp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "etl_last_success_timestamp_seconds", Help: "Unix time of the last successful stage.",
		}, []string{"stage"}),
	}
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.APIRequestsTotal, m.APIRetriesTotal, m.APIRequestDuration,
		m.AssetsRequested, m.AssetsPriced, m.AssetsMissing, m.AssetsStale, m.CoverageDropsTotal, m.PricesChanged, m.PriceJumpsTotal, m.RegistryAmbiguous,
		m.SymbolsExcluded, m.SymbolsUnverified, m.TransformErrorsTotal, m.RowsWrittenTotal, m.ExportRowsTotal, m.ExportCappedTotal,
		m.RunsTotal, m.RunOverrunsTotal, m.LastSuccessTimestamp,
	)
	return m
}
