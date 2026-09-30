// Package observe holds logging, metrics and the health endpoints.
package observe

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
)

// NewLogger returns a JSON slog logger writing to both stdout and the log file
// at path, appending. The file is created along with its directory. Log
// rotation is left to the platform (a container runtime or logrotate), which
// is where it belongs in production.
func NewLogger(path string, level slog.Level) (*slog.Logger, io.Closer, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, nil, fmt.Errorf("create log dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("open log file: %w", err)
	}
	h := slog.NewJSONHandler(io.MultiWriter(os.Stdout, f), &slog.HandlerOptions{
		Level: level,
		// Log clocks are UTC like every other clock in the pipeline.
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				a.Value = slog.TimeValue(a.Value.Time().UTC())
			}
			return a
		},
	})
	return slog.New(h), f, nil
}
