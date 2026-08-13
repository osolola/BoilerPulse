// Command ingest runs the event-ingestion service (spec §8): on an
// interval, it fetches events from a configured EventSource (SimulatorSource
// for now — see internal/events), normalizes them, and POSTs each one to
// the gateway's (or a node's) /v1/events endpoint. If the target is
// unreachable this round, it logs and retries next tick — an ingestion
// hiccup never takes the KV cluster down, and vice versa.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"

	"boilerpulse/internal/events"
	"boilerpulse/internal/logging"
)

func main() {
	_ = godotenv.Load()

	targetURL := os.Getenv("BOILERPULSE_INGEST_TARGET_URL")
	if targetURL == "" {
		targetURL = "http://localhost:8090" // the gateway, by default
	}

	interval := 5 * time.Second
	if v := os.Getenv("BOILERPULSE_INGEST_INTERVAL_SECONDS"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			interval = time.Duration(secs) * time.Second
		}
	}

	logLevel := os.Getenv("BOILERPULSE_LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}

	logger := logging.New(logLevel, "ingest", "ingest")
	source := events.NewSimulatorSource()
	client := &http.Client{Timeout: 5 * time.Second}

	logger.Info("starting event ingestion", "target", targetURL, "interval", interval.String(), "source", source.Name())

	runOnce(logger, client, targetURL, source)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		runOnce(logger, client, targetURL, source)
	}
}

func runOnce(logger *slog.Logger, client *http.Client, targetURL string, source events.EventSource) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	normalized, err := events.FetchAndNormalize(ctx, source)
	if err != nil {
		logger.Error("ingestion round failed", "source", source.Name(), "error", err)
		return
	}

	for _, e := range normalized {
		if err := postEvent(ctx, client, targetURL, e); err != nil {
			logger.Error("failed to post event", "event_id", e.ID, "error", err)
			continue
		}
		logger.Info("ingested event", "event_id", e.ID, "type", e.Type, "urgency", e.Urgency, "title", e.Title)
	}
}

func postEvent(ctx context.Context, client *http.Client, targetURL string, e events.Event) error {
	body, err := json.Marshal(e)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL+"/v1/events", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("unexpected status %d from %s", resp.StatusCode, targetURL)
	}
	return nil
}
