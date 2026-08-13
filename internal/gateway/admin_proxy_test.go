package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// fakeAdmin mimics internal/admin's Server closely enough to exercise the
// gateway's proxy: token-gated /kill, /fault, /restore, /status, tracking
// which paths were hit and with what Authorization header so tests can
// assert the gateway actually forwarded the shared secret.
type fakeAdmin struct {
	server      *httptest.Server
	token       string
	lastAuth    string
	killHits    int
	restoreHits int
}

func newFakeAdmin(token string) *fakeAdmin {
	fa := &fakeAdmin{token: token}
	mux := http.NewServeMux()
	mux.HandleFunc("/kill", func(w http.ResponseWriter, r *http.Request) {
		fa.lastAuth = r.Header.Get("Authorization")
		if fa.lastAuth != "Bearer "+fa.token {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		fa.killHits++
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"status": "killing"})
	})
	mux.HandleFunc("/fault", func(w http.ResponseWriter, r *http.Request) {
		fa.lastAuth = r.Header.Get("Authorization")
		if fa.lastAuth != "Bearer "+fa.token {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"received": string(body)})
	})
	mux.HandleFunc("/restore", func(w http.ResponseWriter, r *http.Request) {
		fa.lastAuth = r.Header.Get("Authorization")
		if fa.lastAuth != "Bearer "+fa.token {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		fa.restoreHits++
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "restored"})
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		fa.lastAuth = r.Header.Get("Authorization")
		if fa.lastAuth != "Bearer "+fa.token {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{"faults": map[string]any{"partitioned": false}, "raft": map[string]any{"state": "FOLLOWER"}})
	})
	fa.server = httptest.NewServer(mux)
	return fa
}

func (fa *fakeAdmin) close() { fa.server.Close() }

func doAdminRequest(t *testing.T, gw *Gateway, method, path, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = bytes.NewBufferString(body)
	}
	req := httptest.NewRequest(method, path, reader)
	req.RemoteAddr = "127.0.0.1:12345"
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, req)
	return rec
}

func adminTestOptions(token string) Options {
	opts := testOptionsNoRateLimit()
	opts.AdminToken = token
	return opts
}

func TestAdminProxyUnauthenticatedRejected(t *testing.T) {
	admin := newFakeAdmin("secret")
	defer admin.close()

	node := Node{ID: "node-1", Addr: "http://unused", AdminAddr: admin.server.URL}
	gw := New([]Node{node}, testLogger(), adminTestOptions("secret"))
	gw.Start(context.Background())
	defer gw.Stop()

	rec := doAdminRequest(t, gw, http.MethodGet, "/v1/admin/node-1/status", "", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestAdminProxyWrongTokenRejected(t *testing.T) {
	admin := newFakeAdmin("secret")
	defer admin.close()

	node := Node{ID: "node-1", Addr: "http://unused", AdminAddr: admin.server.URL}
	gw := New([]Node{node}, testLogger(), adminTestOptions("secret"))
	gw.Start(context.Background())
	defer gw.Stop()

	rec := doAdminRequest(t, gw, http.MethodGet, "/v1/admin/node-1/status", "wrong", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestAdminProxyNoConfiguredTokenDisablesRoutes(t *testing.T) {
	admin := newFakeAdmin("secret")
	defer admin.close()

	node := Node{ID: "node-1", Addr: "http://unused", AdminAddr: admin.server.URL}
	gw := New([]Node{node}, testLogger(), adminTestOptions(""))
	gw.Start(context.Background())
	defer gw.Stop()

	rec := doAdminRequest(t, gw, http.MethodGet, "/v1/admin/node-1/status", "anything", "")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestAdminProxyUnknownNodeReturns404(t *testing.T) {
	admin := newFakeAdmin("secret")
	defer admin.close()

	node := Node{ID: "node-1", Addr: "http://unused", AdminAddr: admin.server.URL}
	gw := New([]Node{node}, testLogger(), adminTestOptions("secret"))
	gw.Start(context.Background())
	defer gw.Stop()

	rec := doAdminRequest(t, gw, http.MethodGet, "/v1/admin/node-99/status", "secret", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestAdminProxyNodeWithNoAdminAddrReturns503(t *testing.T) {
	node := Node{ID: "node-1", Addr: "http://unused"} // AdminAddr left empty
	gw := New([]Node{node}, testLogger(), adminTestOptions("secret"))
	gw.Start(context.Background())
	defer gw.Stop()

	rec := doAdminRequest(t, gw, http.MethodGet, "/v1/admin/node-1/status", "secret", "")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestAdminProxyStatusForwardsToken(t *testing.T) {
	admin := newFakeAdmin("secret")
	defer admin.close()

	node := Node{ID: "node-1", Addr: "http://unused", AdminAddr: admin.server.URL}
	gw := New([]Node{node}, testLogger(), adminTestOptions("secret"))
	gw.Start(context.Background())
	defer gw.Stop()

	rec := doAdminRequest(t, gw, http.MethodGet, "/v1/admin/node-1/status", "secret", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body)
	}
	if admin.lastAuth != "Bearer secret" {
		t.Errorf("node's admin server saw Authorization = %q, want %q", admin.lastAuth, "Bearer secret")
	}
}

func TestAdminProxyKillReachesNode(t *testing.T) {
	admin := newFakeAdmin("secret")
	defer admin.close()

	node := Node{ID: "node-1", Addr: "http://unused", AdminAddr: admin.server.URL}
	gw := New([]Node{node}, testLogger(), adminTestOptions("secret"))
	gw.Start(context.Background())
	defer gw.Stop()

	rec := doAdminRequest(t, gw, http.MethodPost, "/v1/admin/node-1/kill", "secret", "")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusAccepted, rec.Body)
	}
	if admin.killHits != 1 {
		t.Errorf("killHits = %d, want 1", admin.killHits)
	}
}

func TestAdminProxyFaultCarriesBody(t *testing.T) {
	admin := newFakeAdmin("secret")
	defer admin.close()

	node := Node{ID: "node-1", Addr: "http://unused", AdminAddr: admin.server.URL}
	gw := New([]Node{node}, testLogger(), adminTestOptions("secret"))
	gw.Start(context.Background())
	defer gw.Stop()

	rec := doAdminRequest(t, gw, http.MethodPost, "/v1/admin/node-1/fault", "secret", `{"partitioned":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body)
	}
	var got map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["received"] != `{"partitioned":true}` {
		t.Errorf("node received body %q, want %q", got["received"], `{"partitioned":true}`)
	}
}

func TestAdminProxyRestoreReachesNode(t *testing.T) {
	admin := newFakeAdmin("secret")
	defer admin.close()

	node := Node{ID: "node-1", Addr: "http://unused", AdminAddr: admin.server.URL}
	gw := New([]Node{node}, testLogger(), adminTestOptions("secret"))
	gw.Start(context.Background())
	defer gw.Stop()

	rec := doAdminRequest(t, gw, http.MethodPost, "/v1/admin/node-1/restore", "secret", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if admin.restoreHits != 1 {
		t.Errorf("restoreHits = %d, want 1", admin.restoreHits)
	}
}

func TestAdminProxyStatusAllAggregatesAcrossNodes(t *testing.T) {
	admin1 := newFakeAdmin("secret")
	defer admin1.close()
	admin2 := newFakeAdmin("secret")
	defer admin2.close()

	nodes := []Node{
		{ID: "node-1", Addr: "http://unused-1", AdminAddr: admin1.server.URL},
		{ID: "node-2", Addr: "http://unused-2"}, // no admin_addr -- should show up as an error, not fail the request
	}
	gw := New(nodes, testLogger(), adminTestOptions("secret"))
	gw.Start(context.Background())
	defer gw.Stop()

	rec := doAdminRequest(t, gw, http.MethodGet, "/v1/admin/status", "secret", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body)
	}

	var view map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := view["node-1"]; !ok {
		t.Error("response missing node-1")
	}
	var node2 map[string]string
	if err := json.Unmarshal(view["node-2"], &node2); err != nil {
		t.Fatalf("unmarshal node-2: %v", err)
	}
	if node2["error"] == "" {
		t.Error("node-2 (no admin_addr) should report an error, got none")
	}
}
