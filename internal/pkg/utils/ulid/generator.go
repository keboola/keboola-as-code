package ulid

import (
	crand "crypto/rand"
	"io"
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
	entropyReader := ulid.Monotonic(crand.Reader, 0)

	return &defaultGenerator{
		entropy: entropyReader,
	}
}

// NewULID generates a new ULID string.
func (g *defaultGenerator) NewULID() string {
	ms := ulid.Timestamp(time.Now())
	return ulid.MustNew(ms, g.entropy).String()
}
