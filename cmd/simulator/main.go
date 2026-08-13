// Command boilerpulse-sim generates real HTTP load against a running
// BoilerPulse gateway (or a single node), following named traffic-curve
// scenarios (spec §10/§57 Milestone 10), and reports what actually
// happened — never estimated or invented numbers. See simulator/ for the
// scenario definitions and load generator, and benchmarks/ for real
// recorded results from running this.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/joho/godotenv"

	"boilerpulse/simulator"
)

func main() {
	_ = godotenv.Load()

	var (
		scenarioName = flag.String("scenario", "", "scenario to run: normal|finals|athletics|emergency|hotkey|all (required)")
		target       = flag.String("target", "http://localhost:8090", "base URL to load-test (the gateway, normally)")
		topology     = flag.String("topology", "unspecified", "label for the report, e.g. single-node, 3-node, 5-node")
		outPath      = flag.String("out", "", "write the JSON report here (scenario=all writes one file per scenario, suffixed by name); empty prints to stdout only")
		concurrency  = flag.Int("concurrency", 200, "max in-flight requests")
		injectFail   = flag.Bool("inject-failure", false, "kill the current leader partway through the run (requires -target to be the gateway and an admin token)")
		adminToken   = flag.String("admin-token", os.Getenv("BOILERPULSE_ADMIN_TOKEN"), "admin bearer token, for -inject-failure (defaults to $BOILERPULSE_ADMIN_TOKEN)")
	)
	flag.Parse()

	if *scenarioName == "" {
		fmt.Fprintln(os.Stderr, "error: -scenario is required")
		flag.Usage()
		os.Exit(1)
	}

	var scenarios []simulator.Scenario
	if *scenarioName == "all" {
		scenarios = simulator.All()
	} else {
		s, ok := simulator.ByName(*scenarioName)
		if !ok {
			fmt.Fprintf(os.Stderr, "error: unknown scenario %q (want normal, finals, athletics, emergency, hotkey, or all)\n", *scenarioName)
			os.Exit(1)
		}
		scenarios = []simulator.Scenario{s}
	}

	if *injectFail && *adminToken == "" {
		fmt.Fprintln(os.Stderr, "error: -inject-failure requires -admin-token (or $BOILERPULSE_ADMIN_TOKEN)")
		os.Exit(1)
	}

	gen := &simulator.Generator{Target: *target, Concurrency: *concurrency}

	for _, s := range scenarios {
		fmt.Fprintf(os.Stderr, "running %s (%s)...\n", s.Name, s.Description)

		var failure *simulator.FailureInjector
		if *injectFail {
			failure = &simulator.FailureInjector{
				InjectAt: s.RampUp + s.PeakDuration/2,
				Inject:   killCurrentLeader(*target, *adminToken),
			}
		}

		report, err := gen.Run(context.Background(), s, *topology, failure)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %s: %v\n", s.Name, err)
			os.Exit(1)
		}

		fmt.Fprintln(os.Stderr, report.Summary())
		for _, note := range report.Notes {
			fmt.Fprintln(os.Stderr, "  note:", note)
		}

		data, err := report.JSON()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: marshaling report: %v\n", err)
			os.Exit(1)
		}

		if *outPath == "" {
			fmt.Println(string(data))
			continue
		}
		path := *outPath
		if len(scenarios) > 1 {
			path = suffixPath(*outPath, s.Name)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "error: writing %s: %v\n", path, err)
			os.Exit(1)
		}
		fmt.Fprintln(os.Stderr, "wrote", path)
	}
}

// killCurrentLeader resolves the gateway's current leader via GET
// /v1/cluster and returns a FailureInjector.Inject func that kills it
// through the gateway's admin proxy (internal/gateway/admin_proxy.go).
func killCurrentLeader(target, token string) func(ctx context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target+"/v1/cluster", nil)
		if err != nil {
			return "", err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return "", fmt.Errorf("resolving current leader: %w", err)
		}
		defer resp.Body.Close()

		var cluster struct {
			LeaderID string `json:"leader_id"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&cluster); err != nil {
			return "", fmt.Errorf("decoding /v1/cluster: %w", err)
		}
		if cluster.LeaderID == "" {
			return "", fmt.Errorf("no known leader to kill")
		}

		killReq, err := http.NewRequestWithContext(ctx, http.MethodPost, target+"/v1/admin/"+cluster.LeaderID+"/kill", nil)
		if err != nil {
			return "", err
		}
		killReq.Header.Set("Authorization", "Bearer "+token)
		killResp, err := http.DefaultClient.Do(killReq)
		if err != nil {
			return "", fmt.Errorf("killing %s: %w", cluster.LeaderID, err)
		}
		defer killResp.Body.Close()
		if killResp.StatusCode != http.StatusAccepted {
			return "", fmt.Errorf("kill %s: unexpected status %d", cluster.LeaderID, killResp.StatusCode)
		}
		return fmt.Sprintf("killed leader %s", cluster.LeaderID), nil
	}
}

// suffixPath inserts "-<suffix>" before the file extension, e.g.
// ("report.json", "normal") -> "report-normal.json".
func suffixPath(path, suffix string) string {
	if idx := strings.LastIndex(path, "."); idx != -1 {
		return path[:idx] + "-" + suffix + path[idx:]
	}
	return path + "-" + suffix
}
