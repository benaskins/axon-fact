# axon-fact

Event sourcing primitives — Event type, EventStore/Projector/Publisher interfaces.

## Build & Test

```bash
go test ./...
go vet ./...
```

## Key Files

- `fact.go` — core Event type and EventStore/Projector/Publisher interfaces
- `memory.go` — in-memory EventStore implementation
