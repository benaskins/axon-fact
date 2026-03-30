package fact

import "testing"

func TestSchema_FieldByName(t *testing.T) {
	s := Schema{
		Name: "eval_bfcl",
		Fields: []Field{
			{Name: "run_id", Type: String},
			{Name: "model", Type: LowCardinalityString},
			{Name: "pass", Type: Bool},
			{Name: "duration_ms", Type: UInt32},
		},
		OrderBy: []string{"model", "run_id"},
	}

	f, ok := s.FieldByName("model")
	if !ok {
		t.Fatal("expected to find field 'model'")
	}
	if f.Type != LowCardinalityString {
		t.Errorf("expected LowCardinalityString, got %d", f.Type)
	}

	_, ok = s.FieldByName("nonexistent")
	if ok {
		t.Error("expected not to find nonexistent field")
	}
}

func TestFieldType_Values(t *testing.T) {
	// Verify enum values are distinct and ordered.
	types := []FieldType{
		String, LowCardinalityString, Bool,
		UInt16, UInt32, Float32, Float64,
		DateTime64, JSON,
	}
	seen := make(map[FieldType]bool)
	for _, ft := range types {
		if seen[ft] {
			t.Errorf("duplicate FieldType value: %d", ft)
		}
		seen[ft] = true
	}
	if len(seen) != 9 {
		t.Errorf("expected 9 distinct FieldType values, got %d", len(seen))
	}
}

func TestSchema_OrderBy(t *testing.T) {
	s := Schema{
		Name: "events_message",
		Fields: []Field{
			{Name: "timestamp", Type: DateTime64},
			{Name: "agent_slug", Type: LowCardinalityString},
			{Name: "role", Type: LowCardinalityString},
		},
		OrderBy: []string{"agent_slug", "timestamp"},
	}

	if len(s.OrderBy) != 2 {
		t.Fatalf("expected 2 OrderBy fields, got %d", len(s.OrderBy))
	}
	if s.OrderBy[0] != "agent_slug" {
		t.Errorf("expected first OrderBy to be 'agent_slug', got %q", s.OrderBy[0])
	}
}
