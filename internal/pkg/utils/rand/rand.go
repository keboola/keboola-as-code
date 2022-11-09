package rand

import (
	"crypto/rand"
	"encoding/hex"
	"math"
)

// RandomString generates a random string of the specified length.
func RandomString(l uint) string {
	buff := make([]byte, int(math.Ceil(float64(l)/2)))
	rand.Read(buff)
	str := hex.EncodeToString(buff)
	return str[:l]
}
