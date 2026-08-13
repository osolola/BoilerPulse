package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"boilerpulse/internal/raft"
	"boilerpulse/internal/raft/rpc"
)

// noopTransport, memStorage, and noopStateMachine are minimal fakes
// satisfying raft.Transport/Storage/StateMachine -- these tests only need
// a real, constructible *raft.Node to report status from; they never start
// it or exercise the algorithm itself (that's internal/raft's own test
// suite).
type noopTransport struct{}

func (noopTransport) SendRequestVote(ctx context.Context, peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return nil, context.Canceled
}

func (noopTransport) SendAppendEntries(ctx context.Context, peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return nil, context.Canceled
}

type memStorage struct{}

func (memStorage) SaveTermAndVote(term uint64, votedFor string) error { return nil }
func (memStorage) LoadTermAndVote() (uint64, string, error)           { return 0, "", nil }
func (memStorage) AppendEntries(entries []raft.LogEntry) error        { return nil }
func (memStorage) TruncateFrom(index uint64) error                    { return nil }
func (memStorage) LoadLog() ([]raft.LogEntry, error)                  { return nil, nil }

type noopStateMachine struct{}

func (noopStateMachine) Apply(command []byte) error { return nil }

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// testNode returns a real (unstarted) *raft.Node with fake storage and
// transport, just so admin.Server has something to report status from --
// these tests don't exercise Raft behavior itself.
func testNode(t *testing.T) *raft.Node {
	t.Helper()
	node, err := raft.NewNode("node-1", nil, memStorage{}, noopTransport{}, noopStateMachine{}, testLogger(), raft.DefaultOptions())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return node
}

func doRequest(t *testing.T, s *Server, method, path, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = bytes.NewBufferString(body)
	}
	req := httptest.NewRequest(method, path, reader)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

func TestUnauthenticatedRequestRejected(t *testing.T) {
	s := NewServer(testNode(t), rpc.NewFaults(), testLogger(), "secret")
	rec := doRequest(t, s, http.MethodGet, "/status", "", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestWrongTokenRejected(t *testing.T) {
	s := NewServer(testNode(t), rpc.NewFaults(), testLogger(), "secret")
	rec := doRequest(t, s, http.MethodGet, "/status", "wrong", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestNoConfiguredTokenDisablesEndpoints(t *testing.T) {
	s := NewServer(testNode(t), rpc.NewFaults(), testLogger(), "")
	rec := doRequest(t, s, http.MethodGet, "/status", "anything", "")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d (no token configured should disable, not allow)", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestStatusReportsFaultsAndRaftState(t *testing.T) {
	faults := rpc.NewFaults()
	s := NewServer(testNode(t), faults, testLogger(), "secret")

	rec := doRequest(t, s, http.MethodGet, "/status", "secret", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var view statusView
	if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if view.Raft.State != "FOLLOWER" {
		t.Errorf("Raft.State = %q, want %q (fresh node)", view.Raft.State, "FOLLOWER")
	}
	if view.Faults.Partitioned {
		t.Error("Faults.Partitioned = true, want false initially")
	}
}

func TestFaultSetsPartition(t *testing.T) {
	faults := rpc.NewFaults()
	s := NewServer(testNode(t), faults, testLogger(), "secret")

	rec := doRequest(t, s, http.MethodPost, "/fault", "secret", `{"partitioned":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body)
	}

	var view statusView
	json.Unmarshal(rec.Body.Bytes(), &view)
	if !view.Faults.Partitioned {
		t.Error("Faults.Partitioned = false after setting it true, want true")
	}
}

func TestFaultSetsLatencyAndDropRateIndependently(t *testing.T) {
	faults := rpc.NewFaults()
	s := NewServer(testNode(t), faults, testLogger(), "secret")

	doRequest(t, s, http.MethodPost, "/fault", "secret", `{"latency_ms":50}`)
	rec := doRequest(t, s, http.MethodPost, "/fault", "secret", `{"drop_rate":0.5}`)

	var view statusView
	json.Unmarshal(rec.Body.Bytes(), &view)
	if view.Faults.LatencyMS != 50 {
		t.Errorf("LatencyMS = %d, want 50 (should persist across a separate drop_rate-only request)", view.Faults.LatencyMS)
	}
	if view.Faults.DropRate != 0.5 {
		t.Errorf("DropRate = %v, want 0.5", view.Faults.DropRate)
	}
}

func TestRestoreClearsAllFaults(t *testing.T) {
	faults := rpc.NewFaults()
	faults.SetPartitioned(true)
	faults.SetLatency(100 * time.Millisecond)
	faults.SetDropRate(0.9)

	s := NewServer(testNode(t), faults, testLogger(), "secret")
	rec := doRequest(t, s, http.MethodPost, "/restore", "secret", "")

	var view statusView
	json.Unmarshal(rec.Body.Bytes(), &view)
	if view.Faults.Partitioned || view.Faults.LatencyMS != 0 || view.Faults.DropRate != 0 {
		t.Errorf("status after restore = %+v, want all faults cleared", view.Faults)
	}
}

func TestStatusWithNilNodeReportsDisabledRaftWithoutPanicking(t *testing.T) {
	s := NewServer(nil, rpc.NewFaults(), testLogger(), "secret")
	rec := doRequest(t, s, http.MethodGet, "/status", "secret", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var view statusView
	json.Unmarshal(rec.Body.Bytes(), &view)
	if view.Raft.State != "DISABLED" {
		t.Errorf("Raft.State = %q, want %q", view.Raft.State, "DISABLED")
	}
}

func TestKillRespondsThenCallsExit(t *testing.T) {
	s := NewServer(testNode(t), rpc.NewFaults(), testLogger(), "secret")

	exitCalled := make(chan int, 1)
	s.exit = func(code int) { exitCalled <- code } // real code calls os.Exit; substituted here so the test binary survives

	rec := doRequest(t, s, http.MethodPost, "/kill", "secret", "")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusAccepted)
	}

	select {
	case code := <-exitCalled:
		if code != 1 {
			t.Errorf("exit code = %d, want 1 (an ungraceful, non-zero exit -- simulating a crash, not a clean shutdown)", code)
		}
	case <-time.After(time.Second):
		t.Fatal("exit was never called within 1s of a successful /kill response")
	}
}
