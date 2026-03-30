package fact

import "context"

// FieldType identifies the storage type of a schema field.
type FieldType int

const (
	String              FieldType = iota // Variable-length text
	LowCardinalityString                // Text with few distinct values (optimised for columnar stores)
	Bool                                // Boolean
	UInt16                              // Unsigned 16-bit integer
	UInt32                              // Unsigned 32-bit integer
	Float32                             // 32-bit floating point
	Float64                             // 64-bit floating point
	DateTime64                          // Millisecond-precision timestamp
	JSON                                // String holding JSON
)

// Field describes a single column in a schema.
type Field struct {
	Name string
	Type FieldType
}

// Schema describes the shape of a fact type for materialisation.
// The composition root (axon-eval, axon-chat, etc.) defines schemas;
// a Materializer uses them to create tables and insert rows.
type Schema struct {
	// Name identifies this fact type (e.g. "eval_bfcl", "message").
	// Materializers derive table names from this.
	Name string

	// Fields lists the columns in order.
	Fields []Field

	// OrderBy lists field names that define the primary ordering.
	// Materializers use this for CREATE TABLE ORDER BY clauses.
	OrderBy []string
}

// FieldByName returns the field with the given name, or zero Field if not found.
func (s Schema) FieldByName(name string) (Field, bool) {
	for _, f := range s.Fields {
		if f.Name == name {
			return f, true
		}
	}
	return Field{}, false
}

// Fact is a single row of typed data conforming to a Schema.
// Keys correspond to Field.Name values in the schema.
type Fact struct {
	Schema string
	Data   map[string]any
}

// Materializer persists facts to an external analytical store.
// Implementations (e.g. ClickHouse) use Schema metadata to create
// tables and generate insert statements.
type Materializer interface {
	// EnsureSchema creates or migrates storage for the given schemas.
	EnsureSchema(ctx context.Context, schemas ...Schema) error

	// Materialize writes facts to the store. Each fact's Schema field
	// identifies which Schema it conforms to.
	Materialize(ctx context.Context, facts ...Fact) error
}
