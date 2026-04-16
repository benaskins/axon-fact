package fact

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
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

// EventTyper is implemented by domain event structs that can be stored
// as events. Each struct returns a dot-separated type string
// (e.g. "order.created").
type EventTyper interface {
	EventType() string
}

// NewEventID returns a random 32-character hex string suitable for use
// as an event ID.
func NewEventID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%x", b)
}

// NewEvent creates an Event from a domain event struct, marshaling data
// to JSON. Metadata is left nil.
func NewEvent(stream string, data EventTyper) (Event, error) {
	return NewEventWithMeta(stream, data, nil)
}

// NewEventWithMeta creates an Event with optional metadata from a domain
// event struct, marshaling data to JSON.
func NewEventWithMeta(stream string, data EventTyper, meta map[string]string) (Event, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return Event{}, err
	}
	return Event{
		ID:       NewEventID(),
		Stream:   stream,
		Type:     data.EventType(),
		Data:     raw,
		Metadata: meta,
	}, nil
}

// Publisher sends events to an external stream (e.g., NATS JetStream).
// Publishers run asynchronously after Append succeeds — a publish failure
// does not roll back the append.
type Publisher interface {
	Publish(ctx context.Context, events []Event) error
}
