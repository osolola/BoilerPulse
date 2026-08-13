package api

import (
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"boilerpulse/internal/events"
	"boilerpulse/internal/storage"
)

const eventKeyPrefix = "event:"

// consistencyForUrgency ties an event's urgency directly to the spec's
// workload-aware consistency model (§13): CRITICAL events (emergencies,
// severe weather) get CRITICAL consistency, HIGH/SCHEDULED_SPIKE events get
// STRONG, everything else gets EVENTUAL.
func consistencyForUrgency(u events.Urgency) storage.Consistency {
	switch u {
	case events.UrgencyCritical:
		return storage.ConsistencyCritical
	case events.UrgencyHigh, events.UrgencyScheduledSpike:
		return storage.ConsistencyStrong
	default:
		return storage.ConsistencyEventual
	}
}

// handlePostEvent accepts a raw event, runs it through the normalization
// pipeline (internal/events), and stores the result as an ordinary KV entry
// under "event:<id>" — events are real data in the same KV store as
// everything else, not a separate storage system.
func (s *Server) handlePostEvent(w http.ResponseWriter, r *http.Request) {
	var raw events.Event
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeError(w, http.StatusBadRequest, ErrInvalidRequest, "invalid JSON body: "+err.Error())
		return
	}

	normalized, err := events.Normalize(raw)
	if err != nil {
		writeError(w, http.StatusBadRequest, ErrInvalidRequest, err.Error())
		return
	}

	value, err := json.Marshal(normalized)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrInternal, "failed to encode event")
		return
	}

	cmd := storage.Command{
		Op:          storage.CommandSet,
		Key:         eventKeyPrefix + normalized.ID,
		Value:       value,
		Consistency: consistencyForUrgency(normalized.Urgency),
	}
	if !s.applyWrite(w, r, cmd) {
		return
	}

	writeJSON(w, http.StatusCreated, normalized)
}

// handleListEvents returns every currently stored event, sorted by start
// time. It's a full scan of the "event:" key range — fine at this
// project's scale (see docs/storage-engine.md's Scan note), not an indexed
// query engine.
func (s *Server) handleListEvents(w http.ResponseWriter, r *http.Request) {
	entries, err := s.engine.Scan(eventKeyPrefix)
	if err != nil {
		s.logger.Error("event scan failed", "error", err)
		writeError(w, http.StatusInternalServerError, ErrInternal, "failed to list events")
		return
	}

	list := make([]events.Event, 0, len(entries))
	for _, entry := range entries {
		var e events.Event
		if err := json.Unmarshal(entry.Value, &e); err != nil {
			continue // skip a malformed stored value rather than failing the whole list
		}
		list = append(list, e)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].StartTime.Before(list[j].StartTime) })

	writeJSON(w, http.StatusOK, map[string]any{"events": list})
}

// applyWrite proposes cmd through Raft if a Proposer is set, otherwise
// applies it directly to the local engine — the same fallback handlePut and
// handleDelete use, factored out here so handlePostEvent doesn't have to
// duplicate the branch.
func (s *Server) applyWrite(w http.ResponseWriter, r *http.Request, cmd storage.Command) bool {
	if s.proposer != nil {
		return s.writeThroughRaft(w, r, cmd)
	}

	switch cmd.Op {
	case storage.CommandSet:
		if err := s.engine.Put(cmd.Key, cmd.Value, cmd.Consistency, time.Duration(cmd.TTLSeconds)*time.Second); err != nil {
			s.logger.Error("put failed", "key", cmd.Key, "error", err)
			writeError(w, http.StatusInternalServerError, ErrInternal, "failed to store value")
			return false
		}
	case storage.CommandDelete:
		if err := s.engine.Delete(cmd.Key); err != nil {
			s.logger.Error("delete failed", "key", cmd.Key, "error", err)
			writeError(w, http.StatusInternalServerError, ErrInternal, "failed to delete value")
			return false
		}
	}
	return true
}
