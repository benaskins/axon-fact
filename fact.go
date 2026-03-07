// Package fact provides event sourcing primitives: an append-only event store,
// synchronous projectors for building read models, and an async publisher
// interface for streaming events to external consumers.
package fact

import (
	"context"
	"encoding/json"
	"time"
)

// Event is an immutable record of something that happened.
type Event struct {
	ID         string            `json:"id"`
	Stream     string            `json:"stream"`
	Type       string            `json:"type"`
	Data       json.RawMessage   `json:"data"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	Sequence   int64             `json:"sequence"`
	OccurredAt time.Time         `json:"occurred_at"`
}

// EventStore persists and retrieves events.
type EventStore interface {
	// Append persists events to a stream and runs synchronous projections.
	Append(ctx context.Context, stream string, events []Event) error

	// Load returns all events for a stream in sequence order.
	Load(ctx context.Context, stream string) ([]Event, error)

	// LoadFrom returns events for a stream starting after a sequence number.
	LoadFrom(ctx context.Context, stream string, fromSequence int64) ([]Event, error)
}

// Projector processes events to update a read model.
// Projectors run synchronously within Append — the caller sees the
// projected state immediately after the append returns.
type Projector interface {
	Handle(ctx context.Context, event Event) error
}

// Publisher sends events to an external stream (e.g., NATS JetStream).
// Publishers run asynchronously after Append succeeds — a publish failure
// does not roll back the append.
type Publisher interface {
	Publish(ctx context.Context, events []Event) error
}
