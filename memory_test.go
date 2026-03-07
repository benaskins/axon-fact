package fact

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMemoryStore_AppendAndLoad(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	events := []Event{
		{ID: "1", Type: "conversation.created", Data: json.RawMessage(`{}`)},
		{ID: "2", Type: "message.appended", Data: json.RawMessage(`{"role":"user"}`)},
	}

	if err := store.Append(ctx, "conv-1", events); err != nil {
		t.Fatalf("Append: %v", err)
	}

	loaded, err := store.Load(ctx, "conv-1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if len(loaded) != 2 {
		t.Fatalf("got %d events, want 2", len(loaded))
	}
	if loaded[0].Type != "conversation.created" {
		t.Errorf("event[0].Type = %q", loaded[0].Type)
	}
	if loaded[1].Type != "message.appended" {
		t.Errorf("event[1].Type = %q", loaded[1].Type)
	}
}

func TestMemoryStore_SequenceAssignment(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	store.Append(ctx, "conv-1", []Event{{ID: "1", Type: "a", Data: json.RawMessage(`{}`)}})
	store.Append(ctx, "conv-1", []Event{{ID: "2", Type: "b", Data: json.RawMessage(`{}`)}})
	store.Append(ctx, "conv-2", []Event{{ID: "3", Type: "c", Data: json.RawMessage(`{}`)}})

	events, _ := store.Load(ctx, "conv-1")
	if events[0].Sequence != 1 {
		t.Errorf("event[0].Sequence = %d, want 1", events[0].Sequence)
	}
	if events[1].Sequence != 2 {
		t.Errorf("event[1].Sequence = %d, want 2", events[1].Sequence)
	}

	// Different stream has its own sequence
	events2, _ := store.Load(ctx, "conv-2")
	if events2[0].Sequence != 1 {
		t.Errorf("conv-2 event[0].Sequence = %d, want 1", events2[0].Sequence)
	}
}

func TestMemoryStore_StreamAssignment(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	store.Append(ctx, "conv-1", []Event{{ID: "1", Type: "a", Data: json.RawMessage(`{}`)}})

	events, _ := store.Load(ctx, "conv-1")
	if events[0].Stream != "conv-1" {
		t.Errorf("Stream = %q, want conv-1", events[0].Stream)
	}
}

func TestMemoryStore_OccurredAtSet(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	before := time.Now()
	store.Append(ctx, "conv-1", []Event{{ID: "1", Type: "a", Data: json.RawMessage(`{}`)}})
	after := time.Now()

	events, _ := store.Load(ctx, "conv-1")
	if events[0].OccurredAt.Before(before) || events[0].OccurredAt.After(after) {
		t.Errorf("OccurredAt = %v, expected between %v and %v", events[0].OccurredAt, before, after)
	}
}

func TestMemoryStore_OccurredAtPreserved(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.Append(ctx, "conv-1", []Event{{ID: "1", Type: "a", Data: json.RawMessage(`{}`), OccurredAt: ts}})

	events, _ := store.Load(ctx, "conv-1")
	if !events[0].OccurredAt.Equal(ts) {
		t.Errorf("OccurredAt = %v, want %v", events[0].OccurredAt, ts)
	}
}

func TestMemoryStore_LoadFrom(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		store.Append(ctx, "conv-1", []Event{{
			ID:   fmt.Sprintf("%d", i),
			Type: fmt.Sprintf("event-%d", i),
			Data: json.RawMessage(`{}`),
		}})
	}

	// Load from sequence 3 — should return events 4 and 5
	events, err := store.LoadFrom(ctx, "conv-1", 3)
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	if events[0].Sequence != 4 {
		t.Errorf("event[0].Sequence = %d, want 4", events[0].Sequence)
	}
	if events[1].Sequence != 5 {
		t.Errorf("event[1].Sequence = %d, want 5", events[1].Sequence)
	}
}

func TestMemoryStore_LoadFromZero(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	store.Append(ctx, "conv-1", []Event{{ID: "1", Type: "a", Data: json.RawMessage(`{}`)}})

	events, _ := store.LoadFrom(ctx, "conv-1", 0)
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
}

func TestMemoryStore_LoadEmptyStream(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	events, err := store.Load(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("got %d events, want 0", len(events))
	}
}

func TestMemoryStore_StreamIsolation(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	store.Append(ctx, "conv-1", []Event{{ID: "1", Type: "a", Data: json.RawMessage(`{}`)}})
	store.Append(ctx, "conv-2", []Event{{ID: "2", Type: "b", Data: json.RawMessage(`{}`)}})

	events1, _ := store.Load(ctx, "conv-1")
	events2, _ := store.Load(ctx, "conv-2")

	if len(events1) != 1 || events1[0].ID != "1" {
		t.Errorf("conv-1 events: %v", events1)
	}
	if len(events2) != 1 || events2[0].ID != "2" {
		t.Errorf("conv-2 events: %v", events2)
	}
}

func TestMemoryStore_ProjectorRunsSynchronously(t *testing.T) {
	var handled []string
	projector := &funcProjector{fn: func(ctx context.Context, e Event) error {
		handled = append(handled, e.ID)
		return nil
	}}

	store := NewMemoryStore(WithProjector(projector))
	ctx := context.Background()

	store.Append(ctx, "conv-1", []Event{
		{ID: "1", Type: "a", Data: json.RawMessage(`{}`)},
		{ID: "2", Type: "b", Data: json.RawMessage(`{}`)},
	})

	// Projector should have been called before Append returns
	if len(handled) != 2 {
		t.Fatalf("projector handled %d events, want 2", len(handled))
	}
	if handled[0] != "1" || handled[1] != "2" {
		t.Errorf("handled = %v", handled)
	}
}

func TestMemoryStore_MultipleProjectors(t *testing.T) {
	var count1, count2 int
	p1 := &funcProjector{fn: func(ctx context.Context, e Event) error { count1++; return nil }}
	p2 := &funcProjector{fn: func(ctx context.Context, e Event) error { count2++; return nil }}

	store := NewMemoryStore(WithProjector(p1), WithProjector(p2))
	ctx := context.Background()

	store.Append(ctx, "s", []Event{{ID: "1", Type: "a", Data: json.RawMessage(`{}`)}})

	if count1 != 1 || count2 != 1 {
		t.Errorf("count1=%d, count2=%d, want 1,1", count1, count2)
	}
}

func TestMemoryStore_ProjectorError(t *testing.T) {
	projector := &funcProjector{fn: func(ctx context.Context, e Event) error {
		return fmt.Errorf("projection failed")
	}}

	store := NewMemoryStore(WithProjector(projector))
	ctx := context.Background()

	err := store.Append(ctx, "s", []Event{{ID: "1", Type: "a", Data: json.RawMessage(`{}`)}})
	if err == nil {
		t.Fatal("expected error from projector")
	}

	// Events should not be stored if projector fails
	events, _ := store.Load(ctx, "s")
	if len(events) != 0 {
		t.Errorf("got %d events, want 0 (projector failed)", len(events))
	}
}

func TestMemoryStore_PublisherCalledAsync(t *testing.T) {
	var mu sync.Mutex
	var published []Event
	done := make(chan struct{})

	publisher := &funcPublisher{fn: func(ctx context.Context, events []Event) error {
		mu.Lock()
		published = append(published, events...)
		mu.Unlock()
		close(done)
		return nil
	}}

	store := NewMemoryStore(WithPublisher(publisher))
	ctx := context.Background()

	store.Append(ctx, "s", []Event{{ID: "1", Type: "a", Data: json.RawMessage(`{}`)}})

	// Wait for async publish
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("publisher not called within 2s")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(published) != 1 || published[0].ID != "1" {
		t.Errorf("published = %v", published)
	}
}

func TestMemoryStore_PublisherReceivesFullEvents(t *testing.T) {
	done := make(chan []Event, 1)
	publisher := &funcPublisher{fn: func(ctx context.Context, events []Event) error {
		done <- events
		return nil
	}}

	store := NewMemoryStore(WithPublisher(publisher))
	ctx := context.Background()

	store.Append(ctx, "conv-1", []Event{{ID: "1", Type: "message.appended", Data: json.RawMessage(`{}`)}})

	select {
	case events := <-done:
		if events[0].Stream != "conv-1" {
			t.Errorf("Stream = %q", events[0].Stream)
		}
		if events[0].Sequence != 1 {
			t.Errorf("Sequence = %d", events[0].Sequence)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

func TestMemoryStore_AppendEmpty(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	err := store.Append(ctx, "s", nil)
	if err != nil {
		t.Fatalf("Append nil: %v", err)
	}

	err = store.Append(ctx, "s", []Event{})
	if err != nil {
		t.Fatalf("Append empty: %v", err)
	}
}

func TestMemoryStore_ConcurrentAppend(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			store.Append(ctx, "conv-1", []Event{{
				ID:   fmt.Sprintf("%d", n),
				Type: "a",
				Data: json.RawMessage(`{}`),
			}})
		}(i)
	}
	wg.Wait()

	events, _ := store.Load(ctx, "conv-1")
	if len(events) != 100 {
		t.Errorf("got %d events, want 100", len(events))
	}

	// Sequences should be unique and contiguous
	seen := make(map[int64]bool)
	for _, e := range events {
		if seen[e.Sequence] {
			t.Errorf("duplicate sequence %d", e.Sequence)
		}
		seen[e.Sequence] = true
	}
}

// Test helpers

type funcProjector struct {
	fn func(context.Context, Event) error
}

func (p *funcProjector) Handle(ctx context.Context, e Event) error { return p.fn(ctx, e) }

type funcPublisher struct {
	fn func(context.Context, []Event) error
}

func (p *funcPublisher) Publish(ctx context.Context, events []Event) error {
	return p.fn(ctx, events)
}
