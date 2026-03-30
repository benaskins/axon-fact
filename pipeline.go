package fact

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
)

// newID returns a random hex string suitable for event IDs.
func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%x", b)
}

// Pipeline fans out facts to multiple sinks: an EventStore for append-only
// persistence, a Materializer for analytical projections, and Publishers
// for async delivery.
type Pipeline struct {
	store        EventStore
	materializer Materializer
	publishers   []Publisher
	stream       string // EventStore stream name
}

// PipelineOption configures a Pipeline.
type PipelineOption func(*Pipeline)

// WithStore configures the EventStore sink and the stream name facts are
// appended to. If not set, facts are not persisted to an event log.
func WithStore(store EventStore, stream string) PipelineOption {
	return func(p *Pipeline) {
		p.store = store
		p.stream = stream
	}
}

// WithMaterializer configures the Materializer sink.
// If not set, facts are not materialised to an analytical store.
func WithMaterializer(m Materializer) PipelineOption {
	return func(p *Pipeline) {
		p.materializer = m
	}
}

// WithPipelinePublisher adds a Publisher sink. Multiple publishers are
// supported; each runs independently. Publishers fire asynchronously
// after Record returns.
func WithPipelinePublisher(pub Publisher) PipelineOption {
	return func(p *Pipeline) {
		p.publishers = append(p.publishers, pub)
	}
}

// NewPipeline creates a Pipeline with the given options.
func NewPipeline(opts ...PipelineOption) *Pipeline {
	p := &Pipeline{}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// EnsureSchemas delegates to the Materializer to create or migrate
// storage for the given schemas. No-op if no Materializer is configured.
func (p *Pipeline) EnsureSchemas(ctx context.Context, schemas ...Schema) error {
	if p.materializer == nil {
		return nil
	}
	return p.materializer.EnsureSchema(ctx, schemas...)
}

// Record fans out facts to all configured sinks.
//
// Synchronous: EventStore.Append and Materializer.Materialize run
// in sequence. If either fails, Record returns the error immediately.
//
// Asynchronous: Publishers fire in goroutines after synchronous sinks
// succeed. Publish failures are logged but do not fail Record.
func (p *Pipeline) Record(ctx context.Context, facts ...Fact) error {
	if len(facts) == 0 {
		return nil
	}

	if p.store != nil {
		events := make([]Event, len(facts))
		for i, f := range facts {
			data, err := json.Marshal(f.Data)
			if err != nil {
				return fmt.Errorf("marshal fact %d: %w", i, err)
			}
			events[i] = Event{
				ID:         newID(),
				Type:       f.Schema,
				Data:       data,
				OccurredAt: time.Now().UTC(),
			}
		}
		if err := p.store.Append(ctx, p.stream, events); err != nil {
			return fmt.Errorf("event store append: %w", err)
		}

		// Publish asynchronously
		for _, pub := range p.publishers {
			go func(pub Publisher) {
				if err := pub.Publish(ctx, events); err != nil {
					slog.Error("pipeline publish failed", "error", err)
				}
			}(pub)
		}
	}

	if p.materializer != nil {
		if err := p.materializer.Materialize(ctx, facts...); err != nil {
			return fmt.Errorf("materialize: %w", err)
		}
	}

	return nil
}
