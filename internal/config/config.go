// Package config loads per-node configuration from a YAML file, with
// environment variables (BOILERPULSE_*) overriding file values. This mirrors
// the spec's guidance: one YAML file per node plus .env for secrets, no
// framework needed at this scale.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Peer identifies another node in the Raft cluster by ID and the address
// its internal gRPC (Raft) service listens on.
type Peer struct {
	ID       string `yaml:"id"`
	RaftAddr string `yaml:"raft_addr"`
}

// Config is a single node's configuration. Peers is empty for a
// single-node, non-Raft configuration (Milestone 1/2 behavior); cmd/node
// only enables Raft when Peers is non-empty.
type Config struct {
	NodeID   string `yaml:"node_id"`
	HTTPAddr string `yaml:"http_addr"`
	RaftAddr string `yaml:"raft_addr"`
	LogLevel string `yaml:"log_level"`
	DataDir  string `yaml:"data_dir"`
	Peers    []Peer `yaml:"peers"`
	// AdminAddr, if set, starts the chaos/failure-injection admin server
	// (internal/admin) on this address — a separate port from HTTPAddr,
	// never exposed by the public KV API or gateway. Empty means disabled;
	// there's no default, so it must be opted into explicitly per node.
	AdminAddr string `yaml:"admin_addr"`
	// AdminToken gates every admin endpoint. It is intentionally NOT a YAML
	// field — like any secret, it's supplied via BOILERPULSE_ADMIN_TOKEN
	// (env/.env) only, so it's never accidentally committed in a config file.
	AdminToken string `yaml:"-"`
	// CORSOrigin restricts Access-Control-Allow-Origin to this one value.
	// Empty (the default) means "*" -- fine for local dev; a public
	// deployment should set this to the frontend's real origin (see
	// docs/deployment.md).
	CORSOrigin string `yaml:"cors_origin"`
}

// Default returns the configuration used when no file and no env overrides
// are present.
func Default() Config {
	return Config{
		NodeID:   "node-1",
		HTTPAddr: ":8080",
		RaftAddr: ":9080",
		LogLevel: "info",
		DataDir:  "./data/node-1",
	}
}

// Load reads path (if it exists) over Default(), then applies BOILERPULSE_*
// environment variable overrides. A missing file is not an error — it falls
// back to defaults plus env, so the binary runs with zero config present.
func Load(path string) (Config, error) {
	cfg := Default()

	if path != "" {
		data, err := os.ReadFile(path)
		switch {
		case err == nil:
			if err := yaml.Unmarshal(data, &cfg); err != nil {
				return Config{}, fmt.Errorf("parsing config %s: %w", path, err)
			}
		case os.IsNotExist(err):
			// fall through to env overrides on top of defaults
		default:
			return Config{}, fmt.Errorf("reading config %s: %w", path, err)
		}
	}

	applyEnvOverrides(&cfg)
	return cfg, nil
}

func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("BOILERPULSE_NODE_ID"); v != "" {
		cfg.NodeID = v
	}
	if v := os.Getenv("BOILERPULSE_HTTP_ADDR"); v != "" {
		cfg.HTTPAddr = v
	}
	if v := os.Getenv("BOILERPULSE_RAFT_ADDR"); v != "" {
		cfg.RaftAddr = v
	}
	if v := os.Getenv("BOILERPULSE_LOG_LEVEL"); v != "" {
		cfg.LogLevel = v
	}
	if v := os.Getenv("BOILERPULSE_DATA_DIR"); v != "" {
		cfg.DataDir = v
	}
	if v := os.Getenv("BOILERPULSE_ADMIN_ADDR"); v != "" {
		cfg.AdminAddr = v
	}
	cfg.AdminToken = os.Getenv("BOILERPULSE_ADMIN_TOKEN")
	if v := os.Getenv("BOILERPULSE_CORS_ORIGIN"); v != "" {
		cfg.CORSOrigin = v
	}
}

// GatewayNode identifies one KV node the gateway can route to.
type GatewayNode struct {
	ID       string `yaml:"id"`
	HTTPAddr string `yaml:"http_addr"`
	// AdminAddr, if set, is that node's chaos/failure-injection admin
	// server address (see internal/admin) -- lets the gateway proxy
	// kill/fault/restore/status for this node without exposing its admin
	// port directly. Empty means the gateway's admin-proxy routes return
	// 503 for this node.
	AdminAddr string `yaml:"admin_addr"`
}

// GatewayConfig is cmd/gateway's configuration.
type GatewayConfig struct {
	HTTPAddr       string        `yaml:"http_addr"`
	LogLevel       string        `yaml:"log_level"`
	Nodes          []GatewayNode `yaml:"nodes"`
	RateLimitRPS   float64       `yaml:"rate_limit_rps"`
	RateLimitBurst int           `yaml:"rate_limit_burst"`
	// AdminToken gates the gateway's /v1/admin/* proxy routes. Like
	// Config.AdminToken, it is intentionally not a YAML field -- supplied
	// via BOILERPULSE_ADMIN_TOKEN only, the same shared secret every
	// node's admin server expects.
	AdminToken string `yaml:"-"`
	// CORSOrigin restricts Access-Control-Allow-Origin to this one value.
	// Empty (the default) means "*". See Config.CORSOrigin.
	CORSOrigin string `yaml:"cors_origin"`
}

// DefaultGateway returns the configuration used when no file and no env
// overrides are present.
func DefaultGateway() GatewayConfig {
	return GatewayConfig{
		HTTPAddr:       ":8090",
		LogLevel:       "info",
		RateLimitRPS:   50,
		RateLimitBurst: 100,
	}
}

// LoadGateway mirrors Load: reads path (if it exists) over
// DefaultGateway(), then applies env overrides.
func LoadGateway(path string) (GatewayConfig, error) {
	cfg := DefaultGateway()

	if path != "" {
		data, err := os.ReadFile(path)
		switch {
		case err == nil:
			if err := yaml.Unmarshal(data, &cfg); err != nil {
				return GatewayConfig{}, fmt.Errorf("parsing config %s: %w", path, err)
			}
		case os.IsNotExist(err):
			// fall through to env overrides on top of defaults
		default:
			return GatewayConfig{}, fmt.Errorf("reading config %s: %w", path, err)
		}
	}

	applyGatewayEnvOverrides(&cfg)
	return cfg, nil
}

func applyGatewayEnvOverrides(cfg *GatewayConfig) {
	if v := os.Getenv("BOILERPULSE_GATEWAY_HTTP_ADDR"); v != "" {
		cfg.HTTPAddr = v
	}
	if v := os.Getenv("BOILERPULSE_LOG_LEVEL"); v != "" {
		cfg.LogLevel = v
	}
	cfg.AdminToken = os.Getenv("BOILERPULSE_ADMIN_TOKEN")
	if v := os.Getenv("BOILERPULSE_CORS_ORIGIN"); v != "" {
		cfg.CORSOrigin = v
	}
}
