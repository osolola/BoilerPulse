package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"boilerpulse/internal/storage"
)

type putRequest struct {
	Value       json.RawMessage `json:"value"`
	Consistency string          `json:"consistency"`
	TTLSeconds  int64           `json:"ttl_seconds"`
}

type getResponse struct {
	Value       json.RawMessage `json:"value"`
	Consistency string          `json:"consistency"`
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	var req putRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, ErrInvalidRequest, "invalid JSON body: "+err.Error())
		return
	}
	if len(req.Value) == 0 {
		writeError(w, http.StatusBadRequest, ErrInvalidRequest, "\"value\" is required")
		return
	}
	if req.TTLSeconds < 0 {
		writeError(w, http.StatusBadRequest, ErrInvalidRequest, "\"ttl_seconds\" must not be negative")
		return
	}

	consistency := storage.Consistency(req.Consistency)
	switch consistency {
	case "":
		consistency = storage.ConsistencyEventual
	case storage.ConsistencyStrong, storage.ConsistencyEventual, storage.ConsistencyCritical:
	default:
		writeError(w, http.StatusBadRequest, ErrInvalidRequest,
			"\"consistency\" must be one of STRONG, EVENTUAL, CRITICAL")
		return
	}

	cmd := storage.Command{
		Op:          storage.CommandSet,
		Key:         key,
		Value:       req.Value,
		Consistency: consistency,
		TTLSeconds:  req.TTLSeconds,
	}

	if s.proposer != nil {
		if !s.writeThroughRaft(w, r, cmd) {
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}

	ttl := time.Duration(req.TTLSeconds) * time.Second
	if err := s.engine.Put(key, req.Value, consistency, ttl); err != nil {
		s.logger.Error("put failed", "key", key, "error", err)
		writeError(w, http.StatusInternalServerError, ErrInternal, "failed to store value")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	entry, err := s.engine.Get(key)
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			writeError(w, http.StatusNotFound, ErrKeyNotFound, fmt.Sprintf("key %q not found", key))
			return
		}
		s.logger.Error("get failed", "key", key, "error", err)
		writeError(w, http.StatusInternalServerError, ErrInternal, "failed to read value")
		return
	}

	writeJSON(w, http.StatusOK, getResponse{
		Value:       entry.Value,
		Consistency: string(entry.Consistency),
	})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	if s.proposer != nil {
		// Raft's state machine treats deleting an already-gone key as a
		// no-op success (harmless on WAL/log replay), so existence is
		// checked against local applied state first to preserve the
		// KEY_NOT_FOUND behavior clients expect from DELETE.
		if _, err := s.engine.Get(key); err != nil {
			if errors.Is(err, storage.ErrKeyNotFound) {
				writeError(w, http.StatusNotFound, ErrKeyNotFound, fmt.Sprintf("key %q not found", key))
				return
			}
			s.logger.Error("delete pre-check failed", "key", key, "error", err)
			writeError(w, http.StatusInternalServerError, ErrInternal, "failed to delete value")
			return
		}

		cmd := storage.Command{Op: storage.CommandDelete, Key: key}
		if !s.writeThroughRaft(w, r, cmd) {
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if err := s.engine.Delete(key); err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			writeError(w, http.StatusNotFound, ErrKeyNotFound, fmt.Sprintf("key %q not found", key))
			return
		}
		s.logger.Error("delete failed", "key", key, "error", err)
		writeError(w, http.StatusInternalServerError, ErrInternal, "failed to delete value")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// writeThroughRaft proposes cmd and writes an error response (returning
// false) on failure — either because this node isn't the leader, or the
// proposal didn't commit within the request's context. The caller writes
// the success response itself, since PUT and DELETE succeed with different
// (identical, as it happens, both 204) but conceptually distinct bodies.
func (s *Server) writeThroughRaft(w http.ResponseWriter, r *http.Request, cmd storage.Command) bool {
	status := s.proposer.Status()
	if !status.IsLeader {
		writeError(w, http.StatusServiceUnavailable, ErrLeaderUnavailable, notLeaderMessage(status))
		return false
	}

	if err := s.proposer.Propose(r.Context(), cmd); err != nil {
		s.logger.Error("propose failed", "key", cmd.Key, "error", err)
		writeError(w, http.StatusServiceUnavailable, ErrLeaderUnavailable, "failed to commit write: "+err.Error())
		return false
	}
	return true
}

func notLeaderMessage(status RaftStatus) string {
	if status.LeaderID != "" {
		return fmt.Sprintf("this node is not the Raft leader; current leader is %q", status.LeaderID)
	}
	return "this node is not the Raft leader and no leader is currently known"
}
