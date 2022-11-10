package rand

import (
	"crypto/rand"
	"encoding/hex"
	"math"
)

// RandomString generates a random string of the specified length.
func RandomString(l uint) string {
	buff := make([]byte, int(math.Ceil(float64(l)/2)))
	_, err := rand.Read(buff)
	if err != nil {
		// unlikely to happen unless the host system is misconfigured
		panic(err)
	}
	str := hex.EncodeToString(buff)
	return str[:l]
}
