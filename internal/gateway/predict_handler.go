package gateway

import (
	"encoding/json"
	"net/http"

	"boilerpulse/internal/api"
	"boilerpulse/internal/events"
	"boilerpulse/internal/prediction"
)

// handlePredict accepts an event (the same shape POST /v1/events takes) and
// returns a traffic prediction for it (spec §29), without storing
// anything — this is a pure query, unlike POST /v1/events.
func (g *Gateway) handlePredict(w http.ResponseWriter, r *http.Request) {
	if !g.predictor.Trained() {
		writeError(w, http.StatusServiceUnavailable, api.ErrInternal, "prediction model is not available")
		return
	}

	var e events.Event
	if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
		writeError(w, http.StatusBadRequest, api.ErrInvalidRequest, "invalid JSON body: "+err.Error())
		return
	}
	if e.StartTime.IsZero() {
		writeError(w, http.StatusBadRequest, api.ErrInvalidRequest, "\"start_time\" is required")
		return
	}

	output := g.predictor.Predict(prediction.FeaturesFromEvent(e))
	writeJSON(w, http.StatusOK, output)
}

// logPredictionForEventResponse computes and logs a traffic prediction for
// a just-created event, demonstrating the "event ingested -> prediction
// available" flow the spec describes (§29: "The distributed system should
// use this output to change workload behavior"). It only logs today —
// acting on the prediction (proactive capacity changes, §27) is explicitly
// out of scope here; see docs/prediction.md.
func (g *Gateway) logPredictionForEventResponse(body []byte) {
	if !g.predictor.Trained() {
		return
	}
	var e events.Event
	if err := json.Unmarshal(body, &e); err != nil || e.StartTime.IsZero() {
		return
	}

	output := g.predictor.Predict(prediction.FeaturesFromEvent(e))
	g.logger.Info("traffic prediction",
		"event_id", e.ID, "event_type", e.Type, "urgency", e.Urgency,
		"predicted_rps", output.PredictedRPS, "confidence", output.Confidence,
		"recommended_nodes", output.RecommendedNodes, "peak_time", output.PeakTime)
}
