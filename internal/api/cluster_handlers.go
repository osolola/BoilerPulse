package api

import "net/http"

type clusterNode struct {
	ID     string `json:"id"`
	Role   string `json:"role"`
	Status string `json:"status"`
}

type clusterResponse struct {
	Mode  string        `json:"mode"`
	Term  uint64        `json:"term,omitempty"`
	Nodes []clusterNode `json:"nodes"`
}

// handleCluster reports real Raft status when a Proposer is wired up (this
// node's own role/term, plus the leader it currently knows about). Without
// Raft, it honestly reports "SINGLE_NODE" rather than faking a cluster.
//
// Peer health beyond "who does Raft currently believe is leader" isn't
// tracked yet — cross-checking peer liveness independently of Raft's own
// heartbeats is a gateway/observability concern (Milestone 4+), so peer
// entries here are marked "UNKNOWN" rather than asserting something this
// node can't actually verify.
func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	if s.proposer == nil {
		writeJSON(w, http.StatusOK, clusterResponse{
			Mode: "SINGLE_NODE",
			Nodes: []clusterNode{
				{ID: s.nodeID, Role: "LEADER", Status: "HEALTHY"},
			},
		})
		return
	}

	status := s.proposer.Status()
	resp := clusterResponse{
		Mode: "RAFT",
		Term: status.Term,
		Nodes: []clusterNode{
			{ID: s.nodeID, Role: status.State, Status: "HEALTHY"},
		},
	}
	if status.LeaderID != "" && status.LeaderID != s.nodeID {
		resp.Nodes = append(resp.Nodes, clusterNode{ID: status.LeaderID, Role: "LEADER", Status: "UNKNOWN"})
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
