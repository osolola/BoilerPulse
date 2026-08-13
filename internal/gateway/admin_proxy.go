package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"sync"

	"boilerpulse/internal/api"
)

// authenticatedAdmin gates the admin-proxy routes with the same
// shared-secret bearer token each node's own admin server (internal/admin)
// expects -- the gateway does not hold a separate credential, it forwards
// this one token on to whichever node it's proxying to. An empty
// configured token disables every admin route (503), matching
// internal/admin's "opt in deliberately, never on by default" behavior.
func (g *Gateway) authenticatedAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if g.opts.AdminToken == "" {
			writeError(w, http.StatusServiceUnavailable, api.ErrNodeUnavailable, "admin routes are disabled: no admin token configured on the gateway")
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+g.opts.AdminToken {
			writeError(w, http.StatusUnauthorized, api.ErrUnauthorized, "missing or invalid admin token")
			return
		}
		next(w, r)
	}
}

func (g *Gateway) adminNode(id string) *Node {
	for i := range g.nodes {
		if g.nodes[i].ID == id {
			return &g.nodes[i]
		}
	}
	return nil
}

// proxyAdmin forwards the request to nodeID's own admin server at path,
// using the gateway's configured admin token, and copies the response
// back verbatim -- so the frontend (or any client) only ever needs to know
// the gateway's address, not each node's separate admin port.
func (g *Gateway) proxyAdmin(w http.ResponseWriter, r *http.Request, path string) {
	nodeID := r.PathValue("nodeID")
	node := g.adminNode(nodeID)
	if node == nil {
		writeError(w, http.StatusNotFound, api.ErrNodeUnavailable, "unknown node id: "+nodeID)
		return
	}
	if node.AdminAddr == "" {
		writeError(w, http.StatusServiceUnavailable, api.ErrNodeUnavailable, "node "+node.ID+" has no admin_addr configured")
		return
	}

	var body io.Reader
	if r.Body != nil {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, api.ErrInvalidRequest, "reading request body: "+err.Error())
			return
		}
		body = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(r.Context(), r.Method, node.AdminAddr+path, body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, api.ErrInternal, "building admin proxy request: "+err.Error())
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+g.opts.AdminToken)

	resp, err := g.httpClient.Do(req)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, api.ErrNodeUnavailable, "node "+node.ID+" admin server unreachable: "+err.Error())
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (g *Gateway) handleAdminKill(w http.ResponseWriter, r *http.Request) {
	g.proxyAdmin(w, r, "/kill")
}
func (g *Gateway) handleAdminFault(w http.ResponseWriter, r *http.Request) {
	g.proxyAdmin(w, r, "/fault")
}
func (g *Gateway) handleAdminRestore(w http.ResponseWriter, r *http.Request) {
	g.proxyAdmin(w, r, "/restore")
}
func (g *Gateway) handleAdminStatus(w http.ResponseWriter, r *http.Request) {
	g.proxyAdmin(w, r, "/status")
}

// handleAdminStatusAll fans out GET /status to every configured node in
// parallel and reports what each one said, tolerating individual failures
// (an unreachable node shows up as {"error": "..."} rather than failing the
// whole request) -- this is what the frontend's per-node chaos controls
// poll to render fault state for the whole cluster in one request.
type adminStatusResult struct {
	id     string
	status json.RawMessage
	err    string
}

func (g *Gateway) handleAdminStatusAll(w http.ResponseWriter, r *http.Request) {
	results := make([]adminStatusResult, len(g.nodes))
	var wg sync.WaitGroup
	for i, n := range g.nodes {
		wg.Add(1)
		go func(i int, n Node) {
			defer wg.Done()
			results[i] = g.pollAdminStatus(r, n)
		}(i, n)
	}
	wg.Wait()

	view := make(map[string]any, len(results))
	for _, res := range results {
		if res.err != "" {
			view[res.id] = map[string]string{"error": res.err}
			continue
		}
		view[res.id] = res.status
	}
	writeJSON(w, http.StatusOK, view)
}

func (g *Gateway) pollAdminStatus(r *http.Request, n Node) adminStatusResult {
	if n.AdminAddr == "" {
		return adminStatusResult{id: n.ID, err: "no admin_addr configured"}
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, n.AdminAddr+"/status", nil)
	if err != nil {
		return adminStatusResult{id: n.ID, err: err.Error()}
	}
	req.Header.Set("Authorization", "Bearer "+g.opts.AdminToken)

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return adminStatusResult{id: n.ID, err: err.Error()}
	}
	defer resp.Body.Close()

	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return adminStatusResult{id: n.ID, err: "admin server returned " + resp.Status}
	}
	return adminStatusResult{id: n.ID, status: b}
}
