// Command gateway runs the BoilerPulse API gateway: it fronts a configured
// set of KV nodes, routing writes to whichever one is currently the Raft
// leader, distributing reads across all of them, and reporting real
// cluster-wide status (see internal/gateway).
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/joho/godotenv"

	"boilerpulse/internal/config"
	"boilerpulse/internal/gateway"
	"boilerpulse/internal/logging"
)

func main() {
	_ = godotenv.Load()

	configPath := os.Getenv("BOILERPULSE_GATEWAY_CONFIG")
	if configPath == "" {
		configPath = "configs/gateway.yaml"
	}

	cfg, err := config.LoadGateway(configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "failed to load gateway config:", err)
		os.Exit(1)
	}
	if len(cfg.Nodes) == 0 {
		fmt.Fprintln(os.Stderr, "gateway config has no nodes configured")
		os.Exit(1)
	}

	logger := logging.New(cfg.LogLevel, "gateway", "gateway")

	nodes := make([]gateway.Node, len(cfg.Nodes))
	for i, n := range cfg.Nodes {
		nodes[i] = gateway.Node{ID: n.ID, Addr: n.HTTPAddr, AdminAddr: n.AdminAddr}
	}

	opts := gateway.DefaultOptions()
	if cfg.RateLimitRPS > 0 {
		opts.RateLimit = gateway.RateLimitOptions{RequestsPerSecond: cfg.RateLimitRPS, Burst: cfg.RateLimitBurst}
	}
	opts.AdminToken = cfg.AdminToken
	if cfg.AdminToken == "" {
		logger.Warn("BOILERPULSE_ADMIN_TOKEN is not set; gateway admin-proxy routes will respond 503 until it is")
	}
	if cfg.CORSOrigin != "" {
		opts.AllowedOrigin = cfg.CORSOrigin
	} else {
		logger.Warn("BOILERPULSE_CORS_ORIGIN is not set; the gateway allows any browser origin (Access-Control-Allow-Origin: *) -- fine for local dev, not for a public deployment (see docs/deployment.md)")
	}

	gw := gateway.New(nodes, logger, opts)
	gw.Start(context.Background())
	defer gw.Stop()

	logger.Info("starting gateway", "addr", cfg.HTTPAddr, "nodes", len(nodes))
	if err := http.ListenAndServe(cfg.HTTPAddr, gw); err != nil {
		logger.Error("server exited", "error", err)
		os.Exit(1)
	}
}
