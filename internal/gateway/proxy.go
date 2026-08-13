package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"boilerpulse/internal/api"
)

const kvKeyPrefix = "/v1/kv/"

// handleWrite routes PUT/DELETE/event-POSTs to the current leader, with one
// refresh-and-retry if the cached leader turns out to be stale. It reads
// the request body exactly once up front and passes the same bytes to
// every proxy attempt — proxyRequest used to re-read r.Body itself on each
// call, which meant a retry after a stale-leader failure silently sent an
// EMPTY body to the new leader (r.Body is a stream; the first attempt had
// already drained it). Reading it once here and threading it through fixes
// that.
func (g *Gateway) handleWrite(w http.ResponseWriter, r *http.Request) {
	var body []byte
	if r.Body != nil {
		var err error
		body, err = io.ReadAll(r.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, api.ErrInvalidRequest, "failed to read request body")
			return
		}
	}

	g.workload.RecordRequest(kvKeyFromPath(r.URL.Path))

	_, addr, ok := g.currentLeader()
	if !ok {
		g.refresh(r.Context())
		_, addr, ok = g.currentLeader()
		if !ok {
			writeError(w, http.StatusServiceUnavailable, api.ErrQuorumUnavailable,
				"no leader is currently known; the cluster may be electing one")
			return
		}
	}

	status, headers, respBody, delivered := g.proxyRequest(r, body, addr)
	if !delivered {
		// The leader we had cached rejected the write or was unreachable
		// (e.g. it just lost leadership) -- refresh once and retry against
		// whatever we now believe is the leader, rather than making the
		// client immediately retry itself.
		g.refresh(r.Context())
		_, addr, ok = g.currentLeader()
		if !ok {
			writeError(w, http.StatusServiceUnavailable, api.ErrQuorumUnavailable,
				"no leader is currently known after refresh; the cluster may be electing one")
			return
		}
		status, headers, respBody, delivered = g.proxyRequest(r, body, addr)
		if !delivered {
			writeError(w, http.StatusServiceUnavailable, api.ErrQuorumUnavailable,
				"leader unreachable after refresh")
			return
		}
	}

	writeProxiedResponse(w, status, headers, respBody)

	if strings.HasPrefix(r.URL.Path, kvKeyPrefix) {
		g.cache.Delete(r.URL.Path) // the underlying value just changed; drop any stale cached read
	}
	if r.URL.Path == "/v1/events" && status == http.StatusCreated {
		g.maybeSignalCriticalFromEventResponse(respBody)
		g.logPredictionForEventResponse(respBody)
	}
}

// handleRead distributes GETs round-robin across every configured node,
// skipping ahead to the next node on failure (up to one full pass). KV
// reads are served from the gateway's own cache when present; the cache is
// never populated for CRITICAL-consistency values (spec §25).
func (g *Gateway) handleRead(w http.ResponseWriter, r *http.Request) {
	g.workload.RecordRequest(kvKeyFromPath(r.URL.Path))

	cacheable := strings.HasPrefix(r.URL.Path, kvKeyPrefix)
	if cacheable {
		if body, ok := g.cache.Get(r.URL.Path); ok {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body)
			return
		}
	}

	n := len(g.nodes)
	if n == 0 {
		writeError(w, http.StatusServiceUnavailable, api.ErrNodeUnavailable, "no nodes configured")
		return
	}

	start := int(g.readCounter.Add(1)) % n
	for i := 0; i < n; i++ {
		addr := g.nodes[(start+i)%n].Addr
		status, headers, body, delivered := g.proxyRequest(r, nil, addr)
		if !delivered {
			continue
		}
		writeProxiedResponse(w, status, headers, body)
		if cacheable && status == http.StatusOK && isCacheableConsistency(body) {
			g.cache.Set(r.URL.Path, body)
		}
		return
	}
	writeError(w, http.StatusServiceUnavailable, api.ErrNodeUnavailable, "no configured node responded")
}

// proxyRequest forwards r (with the given body) to targetAddr and returns
// the response's status/headers/body without writing anything — callers
// decide whether/how to relay it, since handleWrite and handleRead both
// need to inspect the response before (or instead of) forwarding it
// verbatim. delivered is false if the request couldn't be sent at all, or
// the target answered with its own honest 503 (e.g. LEADER_UNAVAILABLE from
// a node that just lost leadership) -- both are routing failures the caller
// should fail over on, not relay as-is.
func (g *Gateway) proxyRequest(r *http.Request, body []byte, targetAddr string) (status int, headers http.Header, respBody []byte, delivered bool) {
	target := targetAddr + r.URL.Path
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}

	req, err := http.NewRequestWithContext(r.Context(), r.Method, target, bytes.NewReader(body))
	if err != nil {
		return 0, nil, nil, false
	}
	req.Header = r.Header.Clone()

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return 0, nil, nil, false
	}
	defer resp.Body.Close()

	respBody, err = io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, nil, false
	}

	if resp.StatusCode == http.StatusServiceUnavailable {
		return resp.StatusCode, resp.Header, respBody, false
	}
	return resp.StatusCode, resp.Header, respBody, true
}

// writeProxiedResponse relays a proxied node's response to the client,
// skipping CORS headers: the gateway's own ServeHTTP already set the
// correct ones (based on its own AllowedOrigin config) before dispatching
// to the mux, and the proxied node is itself a standalone http.Server that
// sets its own CORS headers on every response regardless of whether it's
// being hit directly or proxied. Copying those through as well used to
// duplicate every Access-Control-* header on any proxied response (GET
// /v1/kv/*, /v1/events, and POST /v1/events all go through here) --
// browsers treat a duplicated Access-Control-Allow-Origin as invalid and
// fail the whole request, even when both copies agree, which is exactly
// what happened here and is why this is a real bug fix, not just cleanup.
func writeProxiedResponse(w http.ResponseWriter, status int, headers http.Header, body []byte) {
	for k, vs := range headers {
		if strings.HasPrefix(k, "Access-Control-") {
			continue
		}
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// isCacheableConsistency reports whether a KV GET response is safe to
// cache: never for CRITICAL consistency (spec §25 — emergency data must
// never be served stale from a cache). The "CRITICAL" literal mirrors
// storage.ConsistencyCritical; duplicated here rather than importing
// internal/storage, matching this package's existing pattern (leader.go) of
// speaking the wire contract directly instead of taking on a new internal
// dependency.
func isCacheableConsistency(body []byte) bool {
	var parsed struct {
		Consistency string `json:"consistency"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return false
	}
	return parsed.Consistency != "CRITICAL"
}

// maybeSignalCriticalFromEventResponse inspects a successful POST
// /v1/events response (the normalized event internal/api returns) and, if
// its computed urgency is CRITICAL, forces the workload engine into
// CRITICAL mode -- spec §26: "Emergency events can directly enter
// CRITICAL." Reading the *response* rather than the request means this
// works even when the client didn't set urgency itself and normalization
// classified it server-side (e.g. every EMERGENCY/WEATHER event the
// simulator posts).
func (g *Gateway) maybeSignalCriticalFromEventResponse(body []byte) {
	var parsed struct {
		Urgency string `json:"urgency"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return
	}
	if parsed.Urgency == "CRITICAL" {
		g.workload.SignalCritical(g.opts.CriticalHoldDuration)
	}
}

func kvKeyFromPath(path string) string {
	if strings.HasPrefix(path, kvKeyPrefix) {
		return path[len(kvKeyPrefix):]
	}
	return ""
}
