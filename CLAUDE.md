@AGENTS.md

## Conventions
- Coarse events with full state snapshots, not fine-grained field diffs
- Synchronous projections (run within Append), async publishing (goroutines)
- Opt-in composition — services work stateless, in-memory, or fully event-sourced
- Adapters (postgres, nats) live in separate repos, not here

## Constraints
- Zero external dependencies — no axon-* modules, no third-party packages
- Do not add database drivers or transport libraries — those belong in adapter repos
- Do not model domain-specific events — this repo provides only the primitives
- EventStore implementations must support transactional append + replay

## Testing
- `go test ./...` — all tests must pass with no external services running
- `go vet ./...` — must be clean
- InMemoryEventStore is the reference implementation; test new interfaces against it
