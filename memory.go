package fact

import (
	"context"
	"sync"
	"time"
)

// MemoryStore is an in-memory EventStore with synchronous projection
// and async publishing. Safe for concurrent use.
type MemoryStore struct {
	mu         sync.Mutex
	streams    map[string][]Event
	projectors []Projector
	publishers []Publisher
}

// Option configures a MemoryStore.
type Option func(*MemoryStore)

// WithProjector registers a projector that runs synchronously on Append.
func WithProjector(p Projector) Option {
	return func(s *MemoryStore) {
		s.projectors = append(s.projectors, p)
	}
}

// WithPublisher registers a publisher that runs asynchronously after Append.
func WithPublisher(p Publisher) Option {
	return func(s *MemoryStore) {
		s.publishers = append(s.publishers, p)
	}
}

// NewMemoryStore creates an in-memory event store.
func NewMemoryStore(opts ...Option) *MemoryStore {
	s := &MemoryStore{
		streams: make(map[string][]Event),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Append stores events in a stream, assigns sequences and timestamps,
// runs projectors synchronously, then publishes asynchronously.
func (s *MemoryStore) Append(ctx context.Context, stream string, events []Event) error {
	if len(events) == 0 {
		return nil
	}

	s.mu.Lock()

	existing := s.streams[stream]
	nextSeq := int64(len(existing)) + 1
	now := time.Now()

	prepared := make([]Event, len(events))
	for i, e := range events {
		e.Stream = stream
		e.Sequence = nextSeq + int64(i)
		if e.OccurredAt.IsZero() {
			e.OccurredAt = now
		}
		prepared[i] = e
	}

	// Run projectors before committing — if any fail, abort
	for _, p := range s.projectors {
		for _, e := range prepared {
			if err := p.Handle(ctx, e); err != nil {
				s.mu.Unlock()
				return err
			}
		}
	}

	s.streams[stream] = append(existing, prepared...)
	s.mu.Unlock()

	// Publish asynchronously
	if len(s.publishers) > 0 {
		go func() {
			for _, pub := range s.publishers {
				pub.Publish(ctx, prepared)
			}
		}()
	}

	return nil
}

// Load returns all events for a stream in sequence order.
func (s *MemoryStore) Load(ctx context.Context, stream string) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	events := s.streams[stream]
	out := make([]Event, len(events))
	copy(out, events)
	return out, nil
}

// LoadFrom returns events for a stream with sequence greater than fromSequence.
func (s *MemoryStore) LoadFrom(ctx context.Context, stream string, fromSequence int64) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	events := s.streams[stream]
	var out []Event
	for _, e := range events {
		if e.Sequence > fromSequence {
			out = append(out, e)
		}
	}
	return out, nil
}
