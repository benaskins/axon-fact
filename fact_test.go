package fact

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestEventStructure(t *testing.T) {
	e := Event{
		ID:         "evt-1",
		Stream:     "conversation-abc",
		Type:       "message.appended",
		Data:       json.RawMessage(`{"role":"user","content":"hello"}`),
		Metadata:   map[string]string{"agent": "writer", "user": "ben"},
		Sequence:   1,
		OccurredAt: time.Now(),
	}

	if e.ID != "evt-1" {
		t.Errorf("ID = %q", e.ID)
	}
	if e.Stream != "conversation-abc" {
		t.Errorf("Stream = %q", e.Stream)
	}
	if e.Type != "message.appended" {
		t.Errorf("Type = %q", e.Type)
	}
	if e.Sequence != 1 {
		t.Errorf("Sequence = %d", e.Sequence)
	}
	if e.Metadata["agent"] != "writer" {
		t.Errorf("Metadata[agent] = %q", e.Metadata["agent"])
	}
}

// Verify interfaces are satisfiable at compile time.
func TestInterfaceCompilation(t *testing.T) {
	var _ EventStore = (*testStore)(nil)
	var _ Projector = (*testProjector)(nil)
	var _ Publisher = (*testPublisher)(nil)
	var _ Materializer = (*testMaterializer)(nil)
}

// Minimal implementations for compile check.
type testStore struct{}

func (s *testStore) Append(ctx context.Context, stream string, events []Event) error {
	return nil
}
func (s *testStore) Load(ctx context.Context, stream string) ([]Event, error) {
	return nil, nil
}
func (s *testStore) LoadFrom(ctx context.Context, stream string, fromSequence int64) ([]Event, error) {
	return nil, nil
}

type testProjector struct{}

func (p *testProjector) Handle(ctx context.Context, event Event) error { return nil }

type testPublisher struct{}

func (p *testPublisher) Publish(ctx context.Context, events []Event) error { return nil }

type testMaterializer struct{}

func (m *testMaterializer) EnsureSchema(ctx context.Context, schemas ...Schema) error { return nil }
func (m *testMaterializer) Materialize(ctx context.Context, facts ...Fact) error               { return nil }

// testEvent implements EventTyper for testing.
type testEvent struct {
	Name string `json:"name"`
}

func (e testEvent) EventType() string { return "test.happened" }

func TestNewEventID(t *testing.T) {
	id := NewEventID()
	if len(id) != 32 {
		t.Errorf("expected 32-char hex string, got %d chars: %q", len(id), id)
	}

	// IDs must be unique
	id2 := NewEventID()
	if id == id2 {
		t.Errorf("two calls returned the same ID: %q", id)
	}
}

func TestNewEvent(t *testing.T) {
	evt, err := NewEvent("order-123", testEvent{Name: "widget"})
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	if evt.Stream != "order-123" {
		t.Errorf("Stream = %q, want %q", evt.Stream, "order-123")
	}
	if evt.Type != "test.happened" {
		t.Errorf("Type = %q, want %q", evt.Type, "test.happened")
	}
	if evt.ID == "" {
		t.Error("ID is empty")
	}
	if len(evt.Metadata) != 0 {
		t.Errorf("Metadata = %v, want nil/empty", evt.Metadata)
	}

	var payload struct{ Name string }
	if err := json.Unmarshal(evt.Data, &payload); err != nil {
		t.Fatalf("unmarshal Data: %v", err)
	}
	if payload.Name != "widget" {
		t.Errorf("Data.Name = %q, want %q", payload.Name, "widget")
	}
}

func TestNewEventWithMeta(t *testing.T) {
	meta := map[string]string{"agent": "writer", "user": "ben"}
	evt, err := NewEventWithMeta("order-123", testEvent{Name: "widget"}, meta)
	if err != nil {
		t.Fatalf("NewEventWithMeta: %v", err)
	}
	if evt.Metadata["agent"] != "writer" {
		t.Errorf("Metadata[agent] = %q, want %q", evt.Metadata["agent"], "writer")
	}
	if evt.Metadata["user"] != "ben" {
		t.Errorf("Metadata[user] = %q, want %q", evt.Metadata["user"], "ben")
	}
}

func TestNewEventMarshalError(t *testing.T) {
	_, err := NewEvent("stream", badEvent{})
	if err == nil {
		t.Fatal("expected marshal error, got nil")
	}
}

// badEvent produces a marshal error.
type badEvent struct{}

func (e badEvent) EventType() string   { return "bad" }
func (e badEvent) MarshalJSON() ([]byte, error) {
	return nil, &json.UnsupportedTypeError{}
}
