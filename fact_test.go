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
