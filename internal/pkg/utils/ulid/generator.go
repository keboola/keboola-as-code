package ulid

import (
	"math/rand"
	"time"

	"github.com/oklog/ulid/v2"
)

// Generator defines an interface for generating ULIDs.
type Generator interface {
	NewULID() string
}

// defaultGenerator is the standard ULID generator using oklog/ulid.
type defaultGenerator struct{}

// NewDefaultGenerator creates a new standard ULID generator.
func NewDefaultGenerator() Generator {
	return &defaultGenerator{}
}

// NewULID generates a new ULID string.
// It replicates the entropy generation logic previously in the executor.
func (g *defaultGenerator) NewULID() string {
	ms := ulid.Timestamp(time.Now())
	// A new rand.Source is created for each ULID to ensure entropy,
	// similar to the original implementation.
	entropy := ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)
	return ulid.MustNew(ms, entropy).String()
}
