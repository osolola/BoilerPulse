package simulator

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// fakeKVServer is a minimal in-memory KV server for exercising Generator
// without a real BoilerPulse cluster -- just enough to verify the load
// generator actually issues the right requests and measures what came
// back.
func fakeKVServer() *httptest.Server {
	var mu sync.Mutex
	store := make(map[string]string)

	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		store[r.PathValue("key")] = "stored"
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /v1/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		_, ok := store[r.PathValue("key")]
		mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"value": "stored"})
	})
	return httptest.NewServer(mux)
}

func testScenario() Scenario {
	return Scenario{
		Name:           "test",
		BaselineRPS:    20,
		PeakRPS:        20,
		PeakDuration:   500 * time.Millisecond,
		WriteRatio:     0.3,
		HotKeyFraction: 0.5,
		HotKeyCount:    1,
		KeySpace:       5,
	}
}

func TestRunAgainstFakeServerAchievesRoughlyTargetRPS(t *testing.T) {
	server := fakeKVServer()
	defer server.Close()

	g := &Generator{Target: server.URL, Concurrency: 50, Rand: rand.New(rand.NewSource(1))}
	report, err := g.Run(context.Background(), testScenario(), "test-topology", nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 20 rps for 500ms should fire roughly 10 requests -- generous bounds
	// since real scheduling jitter and warmup overlap make exact counts
	// unreliable.
	if report.TotalRequests < 3 || report.TotalRequests > 30 {
		t.Errorf("TotalRequests = %d, want roughly 5-15", report.TotalRequests)
	}
	if report.ErrorCount != 0 {
		t.Errorf("ErrorCount = %d, want 0 against a healthy fake server", report.ErrorCount)
	}
	if report.AchievedRPS <= 0 {
		t.Errorf("AchievedRPS = %v, want > 0", report.AchievedRPS)
	}
	if report.Scenario != "test" || report.Topology != "test-topology" {
		t.Errorf("Scenario/Topology = %q/%q, want %q/%q", report.Scenario, report.Topology, "test", "test-topology")
	}
}

func TestWarmupPrimesKeysSoGetsSucceed(t *testing.T) {
	server := fakeKVServer()
	defer server.Close()

	g := &Generator{Target: server.URL, Concurrency: 50, Rand: rand.New(rand.NewSource(1))}
	report, err := g.Run(context.Background(), testScenario(), "test-topology", nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if report.StatusCounts["404"] > 0 {
		t.Errorf("StatusCounts[404] = %d, want 0 -- warmup should have primed every key before timed GETs started", report.StatusCounts["404"])
	}
}

func TestRunInvokesFailureInjectorAtScheduledTime(t *testing.T) {
	server := fakeKVServer()
	defer server.Close()

	var invoked bool
	var invokedAt time.Duration
	start := time.Now()

	failure := &FailureInjector{
		InjectAt: 100 * time.Millisecond,
		Inject: func(ctx context.Context) (string, error) {
			invoked = true
			invokedAt = time.Since(start)
			return "simulated kill", nil
		},
	}

	g := &Generator{Target: server.URL, Concurrency: 50, Rand: rand.New(rand.NewSource(1))}
	report, err := g.Run(context.Background(), testScenario(), "test-topology", failure)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if !invoked {
		t.Fatal("failure injector was never invoked")
	}
	if invokedAt < 50*time.Millisecond {
		t.Errorf("failure injector fired too early: %v (scheduled for 100ms after warmup)", invokedAt)
	}
	if len(report.Notes) == 0 {
		t.Error("report has no notes recording the failure injection")
	}
}

func TestRunInjectorErrorIsRecordedNotFatal(t *testing.T) {
	server := fakeKVServer()
	defer server.Close()

	failure := &FailureInjector{
		InjectAt: 50 * time.Millisecond,
		Inject: func(ctx context.Context) (string, error) {
			return "", fmt.Errorf("boom")
		},
	}

	g := &Generator{Target: server.URL, Concurrency: 50, Rand: rand.New(rand.NewSource(1))}
	report, err := g.Run(context.Background(), testScenario(), "test-topology", failure)
	if err != nil {
		t.Fatalf("Run: %v (an injector error should be recorded in notes, not fail the whole run)", err)
	}
	found := false
	for _, n := range report.Notes {
		if n != "" {
			found = true
		}
	}
	if !found {
		t.Error("expected a note recording the failed injection")
	}
}
