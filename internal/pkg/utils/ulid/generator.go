package ulid

import (
	"crypto/rand"
	"encoding/binary"
	"io"
	"log"
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

// Generates a secure random seed.
func secureRandomSeed() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		log.Fatal("cannot read random seed:", err)
	}
	return binary.BigEndian.Uint64(b[:])
}

// NewDefaultGenerator creates a new standard ULID generator.
func NewDefaultGenerator() Generator {
	entropyReader := ulid.Monotonic(rand.Reader, secureRandomSeed())

	return &defaultGenerator{
		entropy: entropyReader,
	}
}

// NewULID generates a new ULID string.
func (g *defaultGenerator) NewULID() string {
	ms := ulid.Timestamp(time.Now())
	return ulid.MustNew(ms, g.entropy).String()
}
