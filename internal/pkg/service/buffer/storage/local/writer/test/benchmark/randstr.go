package benchmark

import (
	"math/rand"
	"strings"
	"time"
)

const (
	// letterAlphabet is list of allowed letters for the RandomStringGenerator.
	// It's only 5 letters on purpose, because we want to test compression
	// and it doesn't work on completely random data.
	letterAlphabet = "abcde"
	letterIdxBits  = 3                    // letterAlphabet length in bits 5="0b101" -> 3
	letterIdxMask  = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax   = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

type RandomStringGenerator struct {
	src rand.Source
}

func newRandomStringGenerator() *RandomStringGenerator {
	return &RandomStringGenerator{
		src: rand.NewSource(time.Now().UnixNano()),
	}
}

// RandomString method is copied from:
// https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-go
// We need a fast way, with the custom alphabet, it doesn't have to be cryptographically strong.
func (g *RandomStringGenerator) RandomString(n int) string {
	sb := strings.Builder{}
	sb.Grow(n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, g.src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = g.src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterAlphabet) {
			sb.WriteByte(letterAlphabet[idx])
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return sb.String()
}
