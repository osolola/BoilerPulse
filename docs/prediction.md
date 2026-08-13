# Traffic Prediction

`internal/prediction` implements the spec's traffic prediction component
(§28-29): given an event, predict `predicted_rps`, `confidence`,
`peak_time`, and `recommended_nodes` — a small linear-regression model,
deliberately simple per the spec's own guidance ("do not over-focus on
model complexity — the goal is infrastructure adaptation, not winning an ML
competition").

## Why the training data is synthetic, and says so

There is no real Purdue traffic to train on. Spec §67-C requires that this
be stated plainly, not implied — `GenerateSyntheticDataset` builds labeled
`(Features, PeakRPS)` samples from a documented ground-truth formula
(`groundTruthRPS`) plus ~10% noise, not from anything observed. The formula
loosely mirrors the spec's own example scenario magnitudes (§10: normal
~500rps, athletics ~15000rps, emergency ~50000rps) without claiming to
reproduce them exactly. This stands in for what the full traffic simulator
(§10-11, `cmd/simulator`, Milestone 10) would otherwise produce by actually
running many synthetic semesters and observing resulting curves —
generating directly from a formula is a lighter-weight substitute, sufficient
to train and validate a small regression model without that generator
existing yet. The dataset is regenerated fresh in memory every time the
gateway starts (2000 samples by default, `Options.PredictionTrainingSamples`)
— nothing is persisted to disk, so there's no stale synthetic-data file to
maintain or accidentally mistake for real measurements.

## Model

Multiple linear regression, trained via batch gradient descent (2000
iterations) on standardized features (zero mean, unit variance, computed
from the training set itself). Implemented from scratch — no ML library —
matching the project's "build the hard engineering pieces yourself"
principle; a closed-form solution (normal equations) would also work but
needs a matrix inverse, which is more code to get right for no real benefit
at this dataset size.

**Features** (§28's suggested list, reduced to what a normalized `Event`
actually carries): one-hot encoded event type, expected attendance, urgency
(ordinal: NORMAL=0 < HIGH=1 < SCHEDULED_SPIKE=2 < CRITICAL=3), hour of day,
day of week, duration. Geographic concentration and real historical traffic
are not implemented as features — there's no real historical data and no
geo-clustering logic yet.

**Validated behavior** (see `internal/prediction/model_test.go`): the
trained model's squared error on a held-out synthetic test set beats a
mean-only baseline (i.e. it's actually learning the relationship, not just
memorizing); a large athletics event predicts higher RPS than a small
dining event; a CRITICAL-urgency event predicts higher RPS than a NORMAL
one at equal attendance. These are the properties that matter for a
demo-scale model — not a claimed accuracy percentage, which would require
real data to validate against.

`recommended_nodes` is `ceil(predicted_rps / RPSPerNode)`, where
`RPSPerNode` (2000, in `model.go`) is an assumed constant, not a measured
benchmark figure — real measured throughput is Milestone 10's job.
`peak_time` is a naive heuristic (event start + 25% of duration), not a
time-series model.

## Where it's wired in

- `POST /v1/predict` (gateway): accepts an event body (same shape as
  `POST /v1/events`), returns a prediction without storing anything — a
  pure query.
- Every successful `POST /v1/events` also triggers a **logged** prediction
  (`logPredictionForEventResponse`) — demonstrating the "event ingested →
  prediction available" flow the spec describes (§29). It only logs today;
  *acting* on the prediction (proactively scaling capacity, §27) is
  explicitly out of scope — cluster membership is still static (see
  `docs/raft.md`), so there's nothing for a prediction to trigger yet.

## Trying it yourself

```bash
make cluster
curl -X POST localhost:8090/v1/predict -d '{
  "type":"ATHLETICS","title":"Purdue Basketball",
  "expected_attendance":14000,"start_time":"2026-11-20T19:00:00Z"
}'
# {"predicted_rps":53659.7,"confidence":0.85,"peak_time":"2026-11-20T19:15:00Z","recommended_nodes":27}

curl -X POST localhost:8090/v1/predict -d '{
  "type":"DINING","title":"Snack Bar",
  "expected_attendance":50,"start_time":"2026-11-20T12:00:00Z"
}'
# {"predicted_rps":78.9,"confidence":0.85,"peak_time":"2026-11-20T12:15:00Z","recommended_nodes":1}

tail -f .cluster/gateway.log | grep "traffic prediction"  # watch ingest-triggered predictions
make stop
```

Manually verified against a real 3-node cluster + gateway: a large
athletics event predicts ~54k RPS / 27 nodes vs. a small dining event's
~79 RPS / 1 node, and every event `cmd/ingest` posts triggers a logged
prediction — WEATHER/EMERGENCY events (CRITICAL urgency) predicting
~16-17k RPS vs. a NORMAL-urgency TRANSPORTATION event predicting ~878 RPS.

See `docs/event-ingestion.md` for the event pipeline this reads from, and
`docs/workload-model.md` for how workload mode is (separately) driven by
*actual* observed traffic rather than predictions.
