package simulator

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"time"
)

// result is one completed request, recorded internally while a run is in
// progress.
type result struct {
	latency time.Duration
	status  int
	err     bool
}

// Generator issues real HTTP requests against Target (a gateway or node's
// base URL, e.g. "http://localhost:8090") to drive a Scenario's traffic
// curve.
type Generator struct {
	Target string
	Client *http.Client
	// Concurrency bounds how many requests may be in flight at once, so a
	// slow server (rather than the target RPS) becomes the limiting
	// factor once it can't keep up -- which is itself a real, honest
	// measurement (achieved RPS falls below target RPS).
	Concurrency int
	// Rand seeds key/op selection. Pass a seeded *rand.Rand for
	// reproducible runs; nil uses an unseeded source (time-based).
	Rand *rand.Rand
}

func (g *Generator) client() *http.Client {
	if g.Client != nil {
		return g.Client
	}
	return &http.Client{Timeout: 5 * time.Second}
}

func (g *Generator) concurrency() int {
	if g.Concurrency > 0 {
		return g.Concurrency
	}
	return 200
}

func (g *Generator) rng() *rand.Rand {
	if g.Rand != nil {
		return g.Rand
	}
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}

// warmupConcurrency caps how many warmup PUTs may be in flight at once,
// deliberately much lower than a scenario's configured request
// Concurrency. The real cluster's leader serializes every proposal's WAL
// fsync behind one lock (internal/raft/log.go, internal/raft/filestorage.go
// -- correctness-first, no group-commit batching, a documented
// simplification), so firing hundreds of writes at once is a genuine
// pathological case for it, not a realistic one: real traffic arrives
// paced by a target RPS curve (see Scenario.TargetRPS), never as one
// simultaneous burst. Warmup exists purely to prime keys before timing
// starts, so there's no reason to reproduce that burst here.
const warmupConcurrency = 20

// warmup PUTs every key the scenario might touch once, before timing
// starts, so GETs during the timed run hit real data instead of a mix of
// 404s from an empty key space -- a benchmark that counts "key not found"
// as latency/errors would be measuring the wrong thing.
func (g *Generator) warmup(ctx context.Context, s Scenario) error {
	client := g.client()
	sem := make(chan struct{}, warmupConcurrency)
	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	for _, key := range s.allKeys() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			defer func() { <-sem }()
			req, err := http.NewRequestWithContext(ctx, http.MethodPut, g.Target+"/v1/kv/"+key,
				bytes.NewBufferString(`{"value":"sim-warmup","consistency":"EVENTUAL"}`))
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			resp, err := client.Do(req)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			resp.Body.Close()
		}(key)
	}
	wg.Wait()

	select {
	case err := <-errCh:
		return fmt.Errorf("warmup: %w", err)
	default:
		return nil
	}
}

// FailureInjector, if set on a Run, is called once at InjectAt elapsed
// time into the scenario -- used to kill a node mid-run and measure the
// system's behavior through a real failure, not just steady-state load.
type FailureInjector struct {
	InjectAt time.Duration
	Inject   func(ctx context.Context) (note string, err error)
}

// Run executes a warmup pass, then generates load following s's target-RPS
// curve for its full duration, optionally injecting a failure partway
// through, and returns a Report of what actually happened.
func (g *Generator) Run(ctx context.Context, s Scenario, topology string, failure *FailureInjector) (Report, error) {
	if err := g.warmup(ctx, s); err != nil {
		return Report{}, err
	}

	client := g.client()
	rng := g.rng()
	sem := make(chan struct{}, g.concurrency())
	resultsCh := make(chan result, 4096)
	var wg sync.WaitGroup

	start := time.Now()
	stopCh := make(chan struct{})

	var notesMu sync.Mutex
	var notes []string
	if failure != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTimer(failure.InjectAt)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return
			case <-stopCh:
				return
			case <-t.C:
			}
			note, err := failure.Inject(ctx)
			notesMu.Lock()
			defer notesMu.Unlock()
			if err != nil {
				notes = append(notes, fmt.Sprintf("failure injection at %s FAILED: %v", failure.InjectAt, err))
				return
			}
			notes = append(notes, fmt.Sprintf("failure injected at %s: %s", failure.InjectAt, note))
		}()
	}

	// Dispatch loop: every tick, compute how many requests should have
	// fired by now (integral of the target-RPS curve) and fire the
	// difference, so a paused/slow scheduler doesn't under-fire overall.
	const tick = 20 * time.Millisecond
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	var sent float64
	deadline := start.Add(s.TotalDuration())
	for now := range ticker.C {
		elapsed := now.Sub(start)
		if now.After(deadline) {
			break
		}
		targetRate := s.TargetRPS(elapsed)
		sent += targetRate * tick.Seconds()
		for sent >= 1 {
			sent--
			wg.Add(1)
			op := s.pickOp(rng)
			key := s.pickKey(rng)
			go g.fire(ctx, client, sem, &wg, resultsCh, op, key)
		}
	}
	close(stopCh)

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	var results []result
	for r := range resultsCh {
		results = append(results, r)
	}
	actualDuration := time.Since(start)

	notesMu.Lock()
	finalNotes := append([]string(nil), notes...)
	notesMu.Unlock()

	return buildReport(s, topology, start, actualDuration, results, finalNotes), nil
}

func (g *Generator) fire(ctx context.Context, client *http.Client, sem chan struct{}, wg *sync.WaitGroup, out chan<- result, op, key string) {
	defer wg.Done()
	sem <- struct{}{}
	defer func() { <-sem }()

	var req *http.Request
	var err error
	reqStart := time.Now()
	switch op {
	case "PUT":
		req, err = http.NewRequestWithContext(ctx, http.MethodPut, g.Target+"/v1/kv/"+key,
			bytes.NewBufferString(`{"value":"sim-payload","consistency":"EVENTUAL"}`))
	default:
		req, err = http.NewRequestWithContext(ctx, http.MethodGet, g.Target+"/v1/kv/"+key, nil)
	}
	if err != nil {
		out <- result{err: true}
		return
	}

	resp, err := client.Do(req)
	latency := time.Since(reqStart)
	if err != nil {
		out <- result{latency: latency, err: true}
		return
	}
	resp.Body.Close()
	out <- result{latency: latency, status: resp.StatusCode, err: resp.StatusCode >= 500}
}

func buildReport(s Scenario, topology string, start time.Time, duration time.Duration, results []result, notes []string) Report {
	latencies := make([]time.Duration, 0, len(results))
	statusCounts := make(map[string]int, 8)
	errCount := 0
	for _, r := range results {
		if r.err {
			errCount++
			statusCounts["error"]++
			continue
		}
		latencies = append(latencies, r.latency)
		statusCounts[fmt.Sprintf("%d", r.status)]++
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	report := Report{
		Scenario:      s.Name,
		Topology:      topology,
		StartedAt:     start.UTC(),
		DurationMS:    duration.Milliseconds(),
		TotalRequests: len(results),
		ErrorCount:    errCount,
		StatusCounts:  statusCounts,
		Notes:         notes,
	}
	if len(results) > 0 {
		report.ErrorRate = float64(errCount) / float64(len(results))
		report.AchievedRPS = float64(len(results)) / duration.Seconds()
	}
	if len(latencies) > 0 {
		report.LatencyP50Ms = percentile(latencies, 0.50)
		report.LatencyP95Ms = percentile(latencies, 0.95)
		report.LatencyP99Ms = percentile(latencies, 0.99)
		report.LatencyMaxMs = float64(latencies[len(latencies)-1].Microseconds()) / 1000
	}
	return report
}

func percentile(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return float64(sorted[idx].Microseconds()) / 1000
}
