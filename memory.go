package fact

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// memLockKey is a context key indicating the MemoryStore mutex is
// already held by an outer Append call. This enables projectors to
// call Append on other streams without deadlocking.
type memLockKey struct{}

// MemoryStore is an in-memory EventStore with synchronous projection
// and async publishing. Safe for concurrent use.
type MemoryStore struct {
	mu              sync.Mutex
	streams         map[string][]Event
	projectors      []Projector
	publishers      []Publisher
	onPublishError  func(error)
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

// WithPublishErrorHandler registers a callback invoked when a publisher
// returns an error. The callback runs in the publisher goroutine.
// This is in addition to the default slog.Error logging.
func WithPublishErrorHandler(fn func(error)) Option {
	return func(s *MemoryStore) {
		s.onPublishError = fn
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
//
// If called from within a projector (nested Append), the mutex is
// already held and will not be re-acquired. Publishing only runs
// from the outermost Append.
func (s *MemoryStore) Append(ctx context.Context, stream string, events []Event) error {
	if len(events) == 0 {
		return nil
	}

	_, nested := ctx.Value(memLockKey{}).(bool)
	if !nested {
		s.mu.Lock()
	}

	ctx = context.WithValue(ctx, memLockKey{}, true)

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
				if !nested {
					s.mu.Unlock()
				}
				return err
			}
		}
	}

	s.streams[stream] = append(existing, prepared...)

	if !nested {
		s.mu.Unlock()

		// Publish asynchronously — only from the outermost Append
		if len(s.publishers) > 0 {
			go func() {
				for _, pub := range s.publishers {
					if err := pub.Publish(ctx, prepared); err != nil {
						slog.Error("publisher failed", "stream", stream, "error", err)
						if s.onPublishError != nil {
							s.onPublishError(err)
						}
					}
				}
			}()
		}
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
