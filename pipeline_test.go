package fact

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestPipeline_RecordEmpty(t *testing.T) {
	p := NewPipeline()
	if err := p.Record(context.Background()); err != nil {
		t.Fatalf("empty record: %v", err)
	}
}

func TestPipeline_MaterializeOnly(t *testing.T) {
	m := &mockMaterializer{}
	p := NewPipeline(WithMaterializer(m))

	err := p.Record(context.Background(), Fact{
		Schema: "eval_bfcl",
		Data:   map[string]any{"model": "qwen3.5", "pass": true},
	})
	if err != nil {
		t.Fatalf("record: %v", err)
	}

	if len(m.facts) != 1 {
		t.Fatalf("expected 1 fact, got %d", len(m.facts))
	}
	if m.facts[0].Schema != "eval_bfcl" {
		t.Errorf("schema = %q", m.facts[0].Schema)
	}
	if m.facts[0].Data["model"] != "qwen3.5" {
		t.Errorf("data[model] = %v", m.facts[0].Data["model"])
	}
}

func TestPipeline_StoreAndPublish(t *testing.T) {
	store := NewMemoryStore()
	pub := &mockPipelinePublisher{}
	p := NewPipeline(
		WithStore(store, "evals"),
		WithPipelinePublisher(pub),
	)

	err := p.Record(context.Background(), Fact{
		Schema: "eval_bfcl",
		Data:   map[string]any{"pass": true},
	})
	if err != nil {
		t.Fatalf("record: %v", err)
	}

	events, err := store.Load(context.Background(), "evals")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Type != "eval_bfcl" {
		t.Errorf("event type = %q", events[0].Type)
	}

	// Publisher is async — wait briefly.
	time.Sleep(50 * time.Millisecond)
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.events) != 1 {
		t.Fatalf("expected 1 published event, got %d", len(pub.events))
	}
}

func TestPipeline_MaterializeError(t *testing.T) {
	m := &mockMaterializer{err: errors.New("clickhouse down")}
	p := NewPipeline(WithMaterializer(m))

	err := p.Record(context.Background(), Fact{
		Schema: "eval_bfcl",
		Data:   map[string]any{"pass": true},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, m.err) {
		t.Errorf("expected wrapped clickhouse error, got: %v", err)
	}
}

func TestPipeline_StoreError(t *testing.T) {
	store := &failingStore{err: errors.New("pg down")}
	p := NewPipeline(WithStore(store, "evals"))

	err := p.Record(context.Background(), Fact{
		Schema: "eval_bfcl",
		Data:   map[string]any{"pass": true},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPipeline_EnsureSchemas(t *testing.T) {
	m := &mockMaterializer{}
	p := NewPipeline(WithMaterializer(m))

	s := Schema{Name: "eval_bfcl", Fields: []Field{{Name: "pass", Type: Bool}}}
	if err := p.EnsureSchemas(context.Background(), s); err != nil {
		t.Fatalf("ensure schemas: %v", err)
	}
	if len(m.schemas) != 1 {
		t.Fatalf("expected 1 schema, got %d", len(m.schemas))
	}
}

func TestPipeline_EnsureSchemasNoMaterializer(t *testing.T) {
	p := NewPipeline()
	if err := p.EnsureSchemas(context.Background(), Schema{Name: "x"}); err != nil {
		t.Fatalf("expected no-op, got: %v", err)
	}
}

func TestPipeline_MultipleFacts(t *testing.T) {
	m := &mockMaterializer{}
	store := NewMemoryStore()
	p := NewPipeline(WithStore(store, "evals"), WithMaterializer(m))

	err := p.Record(context.Background(),
		Fact{Schema: "eval_bfcl", Data: map[string]any{"case_id": "simple_0"}},
		Fact{Schema: "eval_bfcl", Data: map[string]any{"case_id": "simple_1"}},
		Fact{Schema: "eval_bfcl", Data: map[string]any{"case_id": "simple_2"}},
	)
	if err != nil {
		t.Fatalf("record: %v", err)
	}

	events, _ := store.Load(context.Background(), "evals")
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	if len(m.facts) != 3 {
		t.Fatalf("expected 3 materialised facts, got %d", len(m.facts))
	}
}

// --- test doubles ---

type mockMaterializer struct {
	schemas []Schema
	facts   []Fact
	err     error
}

func (m *mockMaterializer) EnsureSchema(_ context.Context, schemas ...Schema) error {
	m.schemas = append(m.schemas, schemas...)
	return nil
}

func (m *mockMaterializer) Materialize(_ context.Context, facts ...Fact) error {
	if m.err != nil {
		return m.err
	}
	m.facts = append(m.facts, facts...)
	return nil
}

type mockPipelinePublisher struct {
	mu     sync.Mutex
	events []Event
}

func (p *mockPipelinePublisher) Publish(_ context.Context, events []Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, events...)
	return nil
}

type failingStore struct {
	err error
}

func (s *failingStore) Append(_ context.Context, _ string, _ []Event) error {
	return s.err
}
func (s *failingStore) Load(_ context.Context, _ string) ([]Event, error) { return nil, s.err }
func (s *failingStore) LoadFrom(_ context.Context, _ string, _ int64) ([]Event, error) {
	return nil, s.err
}
