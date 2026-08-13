package prediction

import (
	"math"
	"time"

	"boilerpulse/internal/events"
)

// Model is a small multiple linear regression trained via batch gradient
// descent on standardized features. Deliberately simple, per spec §28: "Do
// not over-focus on model complexity — the goal is infrastructure
// adaptation, not winning an ML competition."
type Model struct {
	weights []float64
	mean    []float64
	std     []float64
	trained bool
}

// NewModel returns an untrained model. Call Train before Predict.
func NewModel() *Model {
	return &Model{}
}

// Train fits the model to samples via gradient descent on mean squared
// error, after standardizing each feature (zero mean, unit variance) using
// the training set's own statistics.
func (m *Model) Train(samples []Sample) {
	if len(samples) == 0 {
		return
	}

	n := numFeatures()
	x := make([][]float64, len(samples))
	y := make([]float64, len(samples))
	for i, s := range samples {
		x[i] = s.Features.vector()
		y[i] = s.PeakRPS
	}

	m.mean, m.std = standardizeParams(x, n)
	xs := standardize(x, m.mean, m.std)
	m.weights = gradientDescent(xs, y, n, 2000, 0.05)
	m.trained = true
}

// Trained reports whether Train has been called with at least one sample.
func (m *Model) Trained() bool { return m.trained }

// Output is the prediction result, matching the spec's example shape (§29).
type Output struct {
	PredictedRPS     float64   `json:"predicted_rps"`
	Confidence       float64   `json:"confidence"`
	PeakTime         time.Time `json:"peak_time"`
	RecommendedNodes int       `json:"recommended_nodes"`
}

// RPSPerNode is an assumed steady-state single-node capacity, used only to
// turn a predicted RPS into a recommended node count — a simple heuristic
// (spec §27's "simulate scaling at the application level"), not a measured
// benchmark figure. Real measured throughput arrives in Milestone 10.
const RPSPerNode = 2000.0

// Predict returns a prediction for f. Calling it before Train returns the
// zero Output.
func (m *Model) Predict(f Features) Output {
	if !m.trained {
		return Output{}
	}

	v := f.vector()
	vs := make([]float64, len(v))
	for j, x := range v {
		vs[j] = (x - m.mean[j]) / m.std[j]
	}
	predicted := dot(m.weights, vs)
	if predicted < 0 {
		predicted = 0
	}

	nodes := int(math.Ceil(predicted / RPSPerNode))
	if nodes < 1 {
		nodes = 1
	}

	return Output{
		PredictedRPS:     predicted,
		Confidence:       confidenceFor(f),
		PeakTime:         peakTimeFor(f),
		RecommendedNodes: nodes,
	}
}

// peakTimeFor is a naive heuristic: assume traffic peaks a quarter of the
// way into the event, mirroring the natural ramp-up curve the spec
// describes (§11) without a real time-series model behind it.
func peakTimeFor(f Features) time.Time {
	return f.StartTime.Add(time.Duration(f.DurationMinutes/4) * time.Minute)
}

// confidenceFor is a simple heuristic, not a statistical prediction
// interval: emergencies are treated as inherently less predictable in
// scale (lower confidence), events with real attendance estimates as more
// predictable, everything else as middling.
func confidenceFor(f Features) float64 {
	if f.Urgency == events.UrgencyCritical {
		return 0.5
	}
	if f.ExpectedAttendance > 0 {
		return 0.85
	}
	return 0.6
}

func standardizeParams(x [][]float64, n int) (mean, std []float64) {
	mean = make([]float64, n)
	std = make([]float64, n)

	for j := 0; j < n; j++ {
		var sum float64
		for _, row := range x {
			sum += row[j]
		}
		mean[j] = sum / float64(len(x))
	}
	for j := 0; j < n; j++ {
		var sumSq float64
		for _, row := range x {
			d := row[j] - mean[j]
			sumSq += d * d
		}
		std[j] = math.Sqrt(sumSq / float64(len(x)))
		if std[j] == 0 {
			std[j] = 1
		}
	}

	// Never standardize the bias column: keep it a constant 1 so the
	// learned weight for it is a true intercept.
	mean[0] = 0
	std[0] = 1
	return mean, std
}

func standardize(x [][]float64, mean, std []float64) [][]float64 {
	out := make([][]float64, len(x))
	for i, row := range x {
		out[i] = make([]float64, len(row))
		for j, v := range row {
			out[i][j] = (v - mean[j]) / std[j]
		}
	}
	return out
}

func gradientDescent(x [][]float64, y []float64, n, iterations int, lr float64) []float64 {
	w := make([]float64, n)
	count := float64(len(x))

	for iter := 0; iter < iterations; iter++ {
		grad := make([]float64, n)
		for i, row := range x {
			pred := dot(w, row)
			errVal := pred - y[i]
			for j, v := range row {
				grad[j] += errVal * v
			}
		}
		for j := range w {
			w[j] -= lr * grad[j] / count
		}
	}
	return w
}

func dot(a, b []float64) float64 {
	var sum float64
	for i := range a {
		sum += a[i] * b[i]
	}
	return sum
}
