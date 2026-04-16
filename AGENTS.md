---
module: github.com/benaskins/axon-fact
kind: library
---

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
- `postgres/store.go` — PostgreSQL-backed EventStore with transactional appends and replay
- `postgres/migrations.go` — embedded SQL migrations for the events table
