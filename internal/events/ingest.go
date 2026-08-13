package events

import (
	"context"
	"fmt"
)

// FetchAndNormalize runs one ingestion round for source: fetch, then
// normalize each event, skipping (not failing on) individual events that
// fail validation — one malformed event from a source shouldn't discard
// the rest of the batch.
func FetchAndNormalize(ctx context.Context, source EventSource) ([]Event, error) {
	raw, err := source.FetchEvents(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetching from %s: %w", source.Name(), err)
	}

	normalized := make([]Event, 0, len(raw))
	for _, e := range raw {
		if e.Source == "" {
			e.Source = source.Name()
		}
		n, err := Normalize(e)
		if err != nil {
			continue
		}
		normalized = append(normalized, n)
	}
	return normalized, nil
}
