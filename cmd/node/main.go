// Command node runs a single BoilerPulse KV node: it loads config, wires a
// durable WAL+SSTable storage engine to the KV HTTP API, and — when the
// config lists peers — runs a Raft node alongside it so writes go through
// consensus before they're visible. With no peers configured, it behaves
// exactly like the Milestone 2 single-node binary. There is still no
// gateway (see cmd/gateway/README.md): a client must know which node is
// currently the leader itself.
//
// When AdminAddr is configured, it also starts the chaos/failure-injection
// admin server (internal/admin, spec §23/§34) on that separate port.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"

	"boilerpulse/internal/admin"
	"boilerpulse/internal/api"
	"boilerpulse/internal/config"
	"boilerpulse/internal/logging"
	"boilerpulse/internal/raft"
	raftrpc "boilerpulse/internal/raft/rpc"
	"boilerpulse/internal/storage"
	"boilerpulse/internal/storage/lsm"
	"boilerpulse/pkg/protocol/raftpb"
)

func main() {
	_ = godotenv.Load() // optional; fine if .env is absent

	configPath := os.Getenv("BOILERPULSE_CONFIG")
	if configPath == "" {
		configPath = "configs/node-1.yaml"
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "failed to load config:", err)
		os.Exit(1)
	}

	logger := logging.New(cfg.LogLevel, "kv-node", cfg.NodeID)

	engine, err := lsm.Open(cfg.DataDir, logger, lsm.DefaultOptions())
	if err != nil {
		logger.Error("failed to open storage engine", "data_dir", cfg.DataDir, "error", err)
		os.Exit(1)
	}
	defer engine.Close()

	server := api.NewServer(engine, logger, cfg.NodeID)
	server.SetAllowedOrigin(cfg.CORSOrigin)

	var raftNode *raft.Node
	faults := raftrpc.NewFaults()

	if len(cfg.Peers) > 0 {
		var raftStorage *raft.FileStorage
		var transport *raftrpc.Transport
		var grpcServer *grpc.Server
		raftNode, raftStorage, transport, grpcServer, err = startRaft(cfg, engine, faults, logger)
		if err != nil {
			logger.Error("failed to start raft", "error", err)
			os.Exit(1)
		}
		defer raftNode.Stop()
		defer raftStorage.Close()
		defer transport.Close()
		defer grpcServer.Stop()

		server.SetProposer(&raftProposer{node: raftNode})
	}

	if cfg.AdminAddr != "" {
		startAdmin(cfg, raftNode, faults, logger)
	}

	logger.Info("starting kv-node", "addr", cfg.HTTPAddr, "data_dir", cfg.DataDir, "raft_enabled", len(cfg.Peers) > 0)
	if err := http.ListenAndServe(cfg.HTTPAddr, server); err != nil {
		logger.Error("server exited", "error", err)
		os.Exit(1)
	}
}

// startRaft brings up the Raft node, its gRPC transport and server, and
// starts everything running. The caller is responsible for stopping/closing
// the four returned resources on shutdown. faults is wired into both the
// transport (outgoing RPCs) and the gRPC server (incoming RPCs), so
// chaos-testing controls (internal/admin) affect this node symmetrically.
func startRaft(cfg config.Config, engine storage.Engine, faults *raftrpc.Faults, logger *slog.Logger) (*raft.Node, *raft.FileStorage, *raftrpc.Transport, *grpc.Server, error) {
	peerIDs := make([]string, 0, len(cfg.Peers))
	addrs := make(map[string]string, len(cfg.Peers))
	for _, p := range cfg.Peers {
		peerIDs = append(peerIDs, p.ID)
		addrs[p.ID] = p.RaftAddr
	}

	raftDir := cfg.DataDir + "/raft"
	raftStorage, err := raft.OpenFileStorage(raftDir)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("opening raft storage: %w", err)
	}

	transport := raftrpc.NewTransport(addrs, faults)
	stateMachine := storage.NewRaftStateMachine(engine)

	raftNode, err := raft.NewNode(cfg.NodeID, peerIDs, raftStorage, transport, stateMachine, logger, raft.DefaultOptions())
	if err != nil {
		raftStorage.Close()
		return nil, nil, nil, nil, fmt.Errorf("creating raft node: %w", err)
	}

	lis, err := net.Listen("tcp", cfg.RaftAddr)
	if err != nil {
		raftStorage.Close()
		return nil, nil, nil, nil, fmt.Errorf("listening on raft_addr %s: %w", cfg.RaftAddr, err)
	}

	grpcServer := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(grpcServer, raftrpc.NewServer(raftNode, faults))
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("raft grpc server exited", "error", err)
		}
	}()

	raftNode.Start()
	logger.Info("raft node started", "raft_addr", cfg.RaftAddr, "peers", peerIDs)

	return raftNode, raftStorage, transport, grpcServer, nil
}

// startAdmin starts the chaos/failure-injection admin server on its own
// port. If raftNode is nil (no peers configured, so Raft never started),
// admin still starts but with no Raft status to report — fault injection
// on the (nonexistent) Raft transport is simply a no-op in that case.
func startAdmin(cfg config.Config, raftNode *raft.Node, faults *raftrpc.Faults, logger *slog.Logger) {
	if raftNode == nil {
		logger.Warn("admin server starting without Raft enabled; kill still works, fault injection has nothing to affect")
	}
	if cfg.AdminToken == "" {
		logger.Warn("BOILERPULSE_ADMIN_TOKEN is not set; admin endpoints will respond 503 until it is")
	}

	adminServer := admin.NewServer(raftNode, faults, logger, cfg.AdminToken)
	go func() {
		logger.Info("admin server starting", "addr", cfg.AdminAddr)
		if err := http.ListenAndServe(cfg.AdminAddr, adminServer); err != nil {
			logger.Error("admin server exited", "error", err)
		}
	}()
}

// raftProposer adapts a *raft.Node to api.Proposer.
type raftProposer struct {
	node *raft.Node
}

func (p *raftProposer) Propose(ctx context.Context, cmd storage.Command) error {
	data, err := storage.EncodeCommand(cmd)
	if err != nil {
		return err
	}
	return p.node.Propose(ctx, data)
}

func (p *raftProposer) Status() api.RaftStatus {
	s := p.node.Status()
	return api.RaftStatus{
		State:    s.State.String(),
		Term:     s.Term,
		LeaderID: s.LeaderID,
		IsLeader: s.State == raft.Leader,
	}
}
