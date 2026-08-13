// Package logging provides structured JSON logging via the standard
// library's log/slog, pre-tagged with service and node identity per the
// spec's log shape (level, service, node_id, message, ...).
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// New returns a JSON slog.Logger writing to stdout at the given level,
// pre-populated with "service" and "node_id" fields.
func New(level, service, nodeID string) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(level)})
	return slog.New(handler).With("service", service, "node_id", nodeID)
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
