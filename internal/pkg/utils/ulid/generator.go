package ulid

import (
	"io"
	"math/rand"
	"time"

	"github.com/oklog/ulid/v2"
)

// Generator defines an interface for generating ULIDs.
type Generator interface {
	NewULID() string
}

// defaultGenerator is the standard ULID generator using oklog/ulid.
type defaultGenerator struct {
	entropy io.Reader
}

// NewDefaultGenerator creates a new standard ULID generator.
func NewDefaultGenerator() Generator {
	source := rand.NewSource(time.Now().UnixNano())
	random := rand.New(source)
	entropyReader := ulid.Monotonic(random, 0)

	return &defaultGenerator{
		entropy: entropyReader,
	}
}

// NewULID generates a new ULID string.
func (g *defaultGenerator) NewULID() string {
	ms := ulid.Timestamp(time.Now())
	return ulid.MustNew(ms, g.entropy).String()
}
