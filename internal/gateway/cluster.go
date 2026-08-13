package gateway

import "net/http"

type clusterNodeView struct {
	ID        string `json:"id"`
	Addr      string `json:"addr"`
	Reachable bool   `json:"reachable"`
	Role      string `json:"role,omitempty"`
	Term      uint64 `json:"term,omitempty"`
}

type clusterView struct {
	Mode     string            `json:"mode"`
	LeaderID string            `json:"leader_id,omitempty"`
	Nodes    []clusterNodeView `json:"nodes"`
}

// handleCluster reports a real, gateway's-eye view of the cluster: unlike
// any single node (which only knows its own status plus a leader hint),
// the gateway independently polls every configured node, so it can report
// actual reachability per node, not just what one node believes.
func (g *Gateway) handleCluster(w http.ResponseWriter, r *http.Request) {
	g.mu.RLock()
	leaderID := g.leaderID
	statuses := make(map[string]nodeStatus, len(g.nodeStatus))
	for k, v := range g.nodeStatus {
		statuses[k] = v
	}
	g.mu.RUnlock()

	view := clusterView{Mode: "RAFT_GATEWAY", LeaderID: leaderID}
	for _, n := range g.nodes {
		status := statuses[n.ID]
		view.Nodes = append(view.Nodes, clusterNodeView{
			ID:        n.ID,
			Addr:      n.Addr,
			Reachable: status.reachable,
			Role:      status.role,
			Term:      status.term,
		})
	}

	writeJSON(w, http.StatusOK, view)
}
