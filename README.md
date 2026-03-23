# axon-fact

> Primitives · Part of the [lamina](https://github.com/benaskins/lamina-mono) workspace

Event sourcing primitives for Go. An append-only event store, synchronous projectors for building read models, and an async publisher interface for streaming events to external consumers. Includes in-memory and PostgreSQL-backed implementations. NATS adapters are in the separate axon-nats module.

## Getting started

```bash
go get github.com/benaskins/axon-fact
```

```go
store := fact.NewMemoryStore(
    fact.WithProjector(myProjector),
)

events := []fact.Event{
    {Type: "OrderPlaced", Data: json.RawMessage(`{"item":"widget"}`)},
}

store.Append(ctx, "order-123", events)
```

## Key types

- **`Event`** — immutable record with stream, type, JSON data, metadata, and sequence number
- **`EventStore`** — append-only persistence with `Append`, `Load`, and `LoadFrom`
- **`Projector`** — synchronous event handler that builds read models within `Append`
- **`Publisher`** — async event delivery to external systems (e.g., NATS JetStream)
- **`MemoryStore`** — in-memory `EventStore` implementation with projector and publisher support
- **`PostgresStore`** — PostgreSQL-backed `EventStore` with transactional appends and replay

## License

MIT
