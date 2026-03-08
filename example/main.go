// Example demonstrates basic event sourcing with axon-fact:
// create a store with a projector, append events, and read projected state.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	fact "github.com/benaskins/axon-fact"
)

// OrderTotal is a read model that tracks the running total for an order.
type OrderTotal struct {
	mu    sync.Mutex
	Total int
}

// Handle projects ItemAdded events into a running total.
func (o *OrderTotal) Handle(_ context.Context, e fact.Event) error {
	if e.Type != "ItemAdded" {
		return nil
	}
	var data struct {
		Price int `json:"price"`
	}
	if err := json.Unmarshal(e.Data, &data); err != nil {
		return err
	}
	o.mu.Lock()
	o.Total += data.Price
	o.mu.Unlock()
	return nil
}

func main() {
	ctx := context.Background()
	totals := &OrderTotal{}

	store := fact.NewMemoryStore(
		fact.WithProjector(totals),
	)

	events := []fact.Event{
		{Type: "ItemAdded", Data: json.RawMessage(`{"price":25}`)},
		{Type: "ItemAdded", Data: json.RawMessage(`{"price":15}`)},
	}

	if err := store.Append(ctx, "order-1", events); err != nil {
		log.Fatal(err)
	}

	// Read model is already up to date — projectors run within Append.
	fmt.Printf("Order total: %d\n", totals.Total)

	// Load events back from the store.
	loaded, err := store.Load(ctx, "order-1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Events stored: %d\n", len(loaded))
}
