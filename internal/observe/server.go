package observe

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ReadinessCheck reports whether the service can currently do useful work.
// It returns a human-readable detail map and an error when not ready.
type ReadinessCheck func(ctx context.Context) (map[string]any, error)

// NewServer serves liveness, readiness and metrics.
//
//	GET /healthz  200 while the process runs: liveness.
//	GET /health   alias of /healthz, the name the brief uses.
//	GET /readyz   200 when Postgres answers and the last successful run is fresh; 503 otherwise.
//	GET /metrics  Prometheus exposition.
func NewServer(addr string, reg *prometheus.Registry, ready ReadinessCheck) *http.Server {
	mux := http.NewServeMux()
	live := func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	}
	mux.HandleFunc("GET /healthz", live)
	mux.HandleFunc("GET /health", live)
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		detail, err := ready(ctx)
		if detail == nil {
			detail = map[string]any{}
		}
		if err != nil {
			detail["status"] = "not ready"
			detail["reason"] = err.Error()
			writeJSON(w, http.StatusServiceUnavailable, detail)
			return
		}
		detail["status"] = "ready"
		writeJSON(w, http.StatusOK, detail)
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
