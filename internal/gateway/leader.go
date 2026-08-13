package gateway

import (
	"context"
	"encoding/json"
	"net/http"
)

// clusterPollResponse mirrors internal/api's clusterResponse JSON shape.
// Defined locally rather than imported, since internal/api's response
// types aren't exported (they're a wire contract, not a Go API) — the
// gateway only needs to read the same JSON any other HTTP client would see.
type clusterPollResponse struct {
	Mode  string `json:"mode"`
	Term  uint64 `json:"term"`
	Nodes []struct {
		ID     string `json:"id"`
		Role   string `json:"role"`
		Status string `json:"status"`
	} `json:"nodes"`
}

// refresh polls every configured node's /v1/cluster and updates the
// gateway's view of who the leader is and which nodes are reachable. It
// picks the leader reported by the node with the highest term, since a
// stale partitioned node might still (correctly, per Raft) believe an old
// term's leader is current.
func (g *Gateway) refresh(ctx context.Context) {
	statuses := make(map[string]nodeStatus, len(g.nodes))
	var bestTerm uint64
	var bestLeaderID string

	for _, n := range g.nodes {
		status, ok := g.pollNode(ctx, n)
		statuses[n.ID] = status
		if !ok {
			continue
		}
		if status.role == "LEADER" && status.term >= bestTerm {
			bestTerm = status.term
			bestLeaderID = n.ID
		}
	}

	g.mu.Lock()
	g.nodeStatus = statuses
	if bestLeaderID != "" {
		g.leaderID = bestLeaderID
		g.leaderAddr = g.addrFor(bestLeaderID)
	}
	g.mu.Unlock()
}

func (g *Gateway) pollNode(ctx context.Context, n Node) (nodeStatus, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, n.Addr+"/v1/cluster", nil)
	if err != nil {
		return nodeStatus{}, false
	}
	resp, err := g.httpClient.Do(req)
	if err != nil {
		return nodeStatus{reachable: false}, false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nodeStatus{reachable: false}, false
	}

	var parsed clusterPollResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nodeStatus{reachable: false}, false
	}

	for _, node := range parsed.Nodes {
		if node.ID == n.ID {
			return nodeStatus{reachable: true, role: node.Role, term: parsed.Term}, true
		}
	}
	return nodeStatus{reachable: true}, true
}

// addrFor must be called with g.mu held (write lock; read access is safe
// too since nodes is immutable after construction).
func (g *Gateway) addrFor(id string) string {
	for _, n := range g.nodes {
		if n.ID == id {
			return n.Addr
		}
	}
	return ""
}

// currentLeader returns the best-known leader's address, if any.
func (g *Gateway) currentLeader() (id, addr string, ok bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.leaderID, g.leaderAddr, g.leaderID != ""
}
