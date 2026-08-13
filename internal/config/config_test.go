package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestLoadDefaultsWhenFileMissing(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if !reflect.DeepEqual(cfg, Default()) {
		t.Errorf("Load() = %+v, want defaults %+v", cfg, Default())
	}
}

func TestLoadFromFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node-1.yaml")
	content := "node_id: node-9\nhttp_addr: \":9090\"\nlog_level: debug\ndata_dir: /tmp/node-9-data\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	want := Config{NodeID: "node-9", HTTPAddr: ":9090", RaftAddr: ":9080", LogLevel: "debug", DataDir: "/tmp/node-9-data"}
	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("Load() = %+v, want %+v", cfg, want)
	}
}

func TestLoadEnvOverridesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node-1.yaml")
	if err := os.WriteFile(path, []byte("node_id: node-1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	t.Setenv("BOILERPULSE_NODE_ID", "node-env")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.NodeID != "node-env" {
		t.Errorf("NodeID = %q, want %q", cfg.NodeID, "node-env")
	}
}

func TestLoadPeers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node-1.yaml")
	content := "node_id: node-1\npeers:\n  - id: node-2\n    raft_addr: \"localhost:9081\"\n  - id: node-3\n    raft_addr: \"localhost:9082\"\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	want := []Peer{{ID: "node-2", RaftAddr: "localhost:9081"}, {ID: "node-3", RaftAddr: "localhost:9082"}}
	if !reflect.DeepEqual(cfg.Peers, want) {
		t.Errorf("Peers = %+v, want %+v", cfg.Peers, want)
	}
}

func TestLoadGatewayDefaultsWhenFileMissing(t *testing.T) {
	cfg, err := LoadGateway(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if err != nil {
		t.Fatalf("LoadGateway returned error: %v", err)
	}
	if !reflect.DeepEqual(cfg, DefaultGateway()) {
		t.Errorf("LoadGateway() = %+v, want defaults %+v", cfg, DefaultGateway())
	}
}

func TestLoadGatewayFromFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	content := "http_addr: \":8099\"\nnodes:\n  - id: node-1\n    http_addr: \"http://localhost:8080\"\n  - id: node-2\n    http_addr: \"http://localhost:8081\"\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg, err := LoadGateway(path)
	if err != nil {
		t.Fatalf("LoadGateway returned error: %v", err)
	}
	if cfg.HTTPAddr != ":8099" {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, ":8099")
	}
	want := []GatewayNode{{ID: "node-1", HTTPAddr: "http://localhost:8080"}, {ID: "node-2", HTTPAddr: "http://localhost:8081"}}
	if !reflect.DeepEqual(cfg.Nodes, want) {
		t.Errorf("Nodes = %+v, want %+v", cfg.Nodes, want)
	}
	if cfg.RateLimitRPS != DefaultGateway().RateLimitRPS {
		t.Errorf("RateLimitRPS = %v, want default %v (not overridden in file)", cfg.RateLimitRPS, DefaultGateway().RateLimitRPS)
	}
}

func TestLoadMalformedYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(path, []byte("node_id: [unterminated"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if _, err := Load(path); err == nil {
		t.Error("Load with malformed YAML returned nil error, want error")
	}
}
