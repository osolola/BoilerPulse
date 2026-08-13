package events

import "context"

// EventSource is anything that can produce raw events — a simulator, or
// eventually a public calendar/athletics feed. The system must keep working
// if every configured source is unavailable (spec §8/§40): a source erroring
// just means Ingest skips that source for this round, it never blocks
// ingestion of the others or takes the service down.
type EventSource interface {
	FetchEvents(ctx context.Context) ([]Event, error)
	Name() string
}
