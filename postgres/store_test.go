package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	fact "github.com/benaskins/axon-fact"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://localhost:5432/lamina?sslmode=disable"
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Skipf("skipping postgres test: %v", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		t.Skipf("skipping postgres test: %v", err)
	}

	// Create an isolated schema for this test
	schema := fmt.Sprintf("test_%s", t.Name())
	_, err = db.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %q", schema))
	if err != nil {
		db.Close()
		t.Fatalf("create schema: %v", err)
	}
	_, err = db.Exec(fmt.Sprintf("SET search_path TO %q", schema))
	if err != nil {
		db.Close()
		t.Fatalf("set search_path: %v", err)
	}

	// Run the events table migration directly
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS events (
			id          TEXT NOT NULL,
			stream      TEXT NOT NULL,
			type        TEXT NOT NULL,
			data        JSONB NOT NULL DEFAULT '{}',
			metadata    JSONB NOT NULL DEFAULT '{}',
			sequence    BIGINT NOT NULL,
			occurred_at TIMESTAMPTZ NOT NULL,
			PRIMARY KEY (stream, sequence)
		);
		CREATE INDEX IF NOT EXISTS idx_events_stream
			ON events(stream, sequence);
	`)
	if err != nil {
		db.Close()
		t.Fatalf("run migration: %v", err)
	}

	t.Cleanup(func() {
		db.Exec(fmt.Sprintf("DROP SCHEMA %q CASCADE", schema))
		db.Close()
	})

	return db
}

type funcProjector struct {
	fn func(context.Context, fact.Event) error
}

func (p *funcProjector) Handle(ctx context.Context, e fact.Event) error { return p.fn(ctx, e) }

func TestStore_Append(t *testing.T) {
	db := openTestDB(t)
	store := NewStore(db)
	ctx := context.Background()

	events := []fact.Event{
		{ID: "e1", Type: "item.added", Data: json.RawMessage(`{"price":25}`)},
	}

	if err := store.Append(ctx, "order-1", events); err != nil {
		t.Fatalf("Append: %v", err)
	}

	loaded, err := store.Load(ctx, "order-1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if len(loaded) != 1 {
		t.Fatalf("got %d events, want 1", len(loaded))
	}
	if loaded[0].Stream != "order-1" {
		t.Errorf("Stream = %q, want %q", loaded[0].Stream, "order-1")
	}
	if loaded[0].Type != "item.added" {
		t.Errorf("Type = %q, want %q", loaded[0].Type, "item.added")
	}
	if loaded[0].Sequence != 1 {
		t.Errorf("Sequence = %d, want 1", loaded[0].Sequence)
	}
	if loaded[0].OccurredAt.IsZero() {
		t.Error("expected non-zero OccurredAt")
	}
}

func TestStore_AppendMultiple(t *testing.T) {
	db := openTestDB(t)
	store := NewStore(db)
	ctx := context.Background()

	store.Append(ctx, "s1", []fact.Event{
		{ID: "e1", Type: "a", Data: json.RawMessage(`{}`)},
	})
	store.Append(ctx, "s1", []fact.Event{
		{ID: "e2", Type: "b", Data: json.RawMessage(`{}`)},
	})

	loaded, err := store.Load(ctx, "s1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if len(loaded) != 2 {
		t.Fatalf("got %d events, want 2", len(loaded))
	}
	if loaded[0].Sequence != 1 {
		t.Errorf("event[0].Sequence = %d, want 1", loaded[0].Sequence)
	}
	if loaded[1].Sequence != 2 {
		t.Errorf("event[1].Sequence = %d, want 2", loaded[1].Sequence)
	}
}

func TestStore_LoadFrom(t *testing.T) {
	db := openTestDB(t)
	store := NewStore(db)
	ctx := context.Background()

	store.Append(ctx, "s1", []fact.Event{
		{ID: "e1", Type: "a", Data: json.RawMessage(`{}`)},
		{ID: "e2", Type: "b", Data: json.RawMessage(`{}`)},
		{ID: "e3", Type: "c", Data: json.RawMessage(`{}`)},
	})

	loaded, err := store.LoadFrom(ctx, "s1", 1)
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}

	if len(loaded) != 2 {
		t.Fatalf("got %d events, want 2", len(loaded))
	}
	if loaded[0].Sequence != 2 {
		t.Errorf("event[0].Sequence = %d, want 2", loaded[0].Sequence)
	}
	if loaded[1].Sequence != 3 {
		t.Errorf("event[1].Sequence = %d, want 3", loaded[1].Sequence)
	}
}

func TestStore_LoadEmpty(t *testing.T) {
	db := openTestDB(t)
	store := NewStore(db)
	ctx := context.Background()

	loaded, err := store.Load(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 0 {
		t.Fatalf("got %d events, want 0", len(loaded))
	}
}

func TestStore_StreamIsolation(t *testing.T) {
	db := openTestDB(t)
	store := NewStore(db)
	ctx := context.Background()

	store.Append(ctx, "s1", []fact.Event{
		{ID: "e1", Type: "a", Data: json.RawMessage(`{}`)},
	})
	store.Append(ctx, "s2", []fact.Event{
		{ID: "e2", Type: "b", Data: json.RawMessage(`{}`)},
	})

	loaded1, _ := store.Load(ctx, "s1")
	loaded2, _ := store.Load(ctx, "s2")

	if len(loaded1) != 1 {
		t.Errorf("s1: got %d events, want 1", len(loaded1))
	}
	if len(loaded2) != 1 {
		t.Errorf("s2: got %d events, want 1", len(loaded2))
	}
	if loaded1[0].Sequence != 1 {
		t.Errorf("s1 sequence = %d, want 1", loaded1[0].Sequence)
	}
	if loaded2[0].Sequence != 1 {
		t.Errorf("s2 sequence = %d, want 1", loaded2[0].Sequence)
	}
}

func TestStore_Metadata(t *testing.T) {
	db := openTestDB(t)
	store := NewStore(db)
	ctx := context.Background()

	store.Append(ctx, "s1", []fact.Event{
		{
			ID:       "e1",
			Type:     "a",
			Data:     json.RawMessage(`{}`),
			Metadata: map[string]string{"user": "ben", "source": "cli"},
		},
	})

	loaded, err := store.Load(ctx, "s1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if loaded[0].Metadata["user"] != "ben" {
		t.Errorf("Metadata[user] = %q, want %q", loaded[0].Metadata["user"], "ben")
	}
	if loaded[0].Metadata["source"] != "cli" {
		t.Errorf("Metadata[source] = %q, want %q", loaded[0].Metadata["source"], "cli")
	}
}

func TestStore_LoadAll(t *testing.T) {
	db := openTestDB(t)
	store := NewStore(db)
	ctx := context.Background()

	store.Append(ctx, "s1", []fact.Event{
		{ID: "e1", Type: "a", Data: json.RawMessage(`{}`)},
	})
	store.Append(ctx, "s2", []fact.Event{
		{ID: "e2", Type: "b", Data: json.RawMessage(`{}`)},
	})
	store.Append(ctx, "s1", []fact.Event{
		{ID: "e3", Type: "c", Data: json.RawMessage(`{}`)},
	})

	all, err := store.LoadAll(ctx)
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}

	if len(all) != 3 {
		t.Fatalf("got %d events, want 3", len(all))
	}

	for i := 1; i < len(all); i++ {
		if all[i].OccurredAt.Before(all[i-1].OccurredAt) {
			t.Errorf("event[%d] occurred before event[%d]", i, i-1)
		}
	}
}

func TestStore_Replay(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	var count1 int
	p1 := &funcProjector{fn: func(_ context.Context, e fact.Event) error {
		count1++
		return nil
	}}
	store1 := NewStore(db, WithProjector(p1))

	store1.Append(ctx, "s1", []fact.Event{
		{ID: "e1", Type: "a", Data: json.RawMessage(`{}`)},
		{ID: "e2", Type: "b", Data: json.RawMessage(`{}`)},
	})

	if count1 != 2 {
		t.Fatalf("projector handled %d events during append, want 2", count1)
	}

	var count2 int
	p2 := &funcProjector{fn: func(_ context.Context, e fact.Event) error {
		count2++
		return nil
	}}
	store2 := NewStore(db, WithProjector(p2))

	if err := store2.Replay(ctx); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if count2 != 2 {
		t.Errorf("projector handled %d events during replay, want 2", count2)
	}
}

func TestStore_ProjectorError(t *testing.T) {
	db := openTestDB(t)
	store := NewStore(db, WithProjector(&funcProjector{
		fn: func(_ context.Context, e fact.Event) error {
			return fmt.Errorf("projection failed")
		},
	}))
	ctx := context.Background()

	err := store.Append(ctx, "s1", []fact.Event{
		{ID: "e1", Type: "a", Data: json.RawMessage(`{}`)},
	})
	if err == nil {
		t.Fatal("expected error from projector")
	}

	store2 := NewStore(db)
	loaded, _ := store2.Load(ctx, "s1")
	if len(loaded) != 0 {
		t.Errorf("got %d events, want 0 (projector failed, tx rolled back)", len(loaded))
	}
}

func TestStore_AppendEmpty(t *testing.T) {
	db := openTestDB(t)
	store := NewStore(db)
	ctx := context.Background()

	if err := store.Append(ctx, "s1", nil); err != nil {
		t.Fatalf("Append nil: %v", err)
	}
	if err := store.Append(ctx, "s1", []fact.Event{}); err != nil {
		t.Fatalf("Append empty: %v", err)
	}
}

func TestStore_NestedAppend(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	var store *Store
	reactor := &funcProjector{fn: func(ctx context.Context, e fact.Event) error {
		if e.Type == "trigger" {
			return store.Append(ctx, "derived", []fact.Event{
				{ID: e.ID + "-derived", Type: "derived.event", Data: json.RawMessage(`{}`)},
			})
		}
		return nil
	}}

	store = NewStore(db, WithProjector(reactor))

	err := store.Append(ctx, "source", []fact.Event{
		{ID: "e1", Type: "trigger", Data: json.RawMessage(`{}`)},
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	source, _ := store.Load(ctx, "source")
	derived, _ := store.Load(ctx, "derived")

	if len(source) != 1 {
		t.Errorf("source: got %d events, want 1", len(source))
	}
	if len(derived) != 1 {
		t.Errorf("derived: got %d events, want 1", len(derived))
	}
	if derived[0].ID != "e1-derived" {
		t.Errorf("derived event ID = %q, want %q", derived[0].ID, "e1-derived")
	}
	if derived[0].Stream != "derived" {
		t.Errorf("derived event Stream = %q, want %q", derived[0].Stream, "derived")
	}
	if derived[0].Sequence != 1 {
		t.Errorf("derived event Sequence = %d, want 1", derived[0].Sequence)
	}
}

func TestStore_NestedAppendAtomicity(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	failProjector := &funcProjector{fn: func(ctx context.Context, e fact.Event) error {
		if e.Type == "derived.event" {
			return fmt.Errorf("derived projection failed")
		}
		return nil
	}}

	var store *Store
	reactor := &funcProjector{fn: func(ctx context.Context, e fact.Event) error {
		if e.Type == "trigger" {
			return store.Append(ctx, "derived", []fact.Event{
				{ID: e.ID + "-derived", Type: "derived.event", Data: json.RawMessage(`{}`)},
			})
		}
		return nil
	}}

	store = NewStore(db, WithProjector(reactor), WithProjector(failProjector))

	err := store.Append(ctx, "source", []fact.Event{
		{ID: "e1", Type: "trigger", Data: json.RawMessage(`{}`)},
	})
	if err == nil {
		t.Fatal("expected error from nested projector")
	}

	store2 := NewStore(db)
	source, _ := store2.Load(ctx, "source")
	derived, _ := store2.Load(ctx, "derived")

	if len(source) != 0 {
		t.Errorf("source: got %d events, want 0 (tx rolled back)", len(source))
	}
	if len(derived) != 0 {
		t.Errorf("derived: got %d events, want 0 (tx rolled back)", len(derived))
	}
}

// Verify Store satisfies the fact.EventStore interface at compile time.
var _ fact.EventStore = (*Store)(nil)
