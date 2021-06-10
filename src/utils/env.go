package utils

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func MustGetEnv(key string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		panic(fmt.Errorf("missing ENV variable \"%s\"", key))
	}
	return value
}

// ResetEnv used from https://golang.org/src/os/env_test.go
func ResetEnv(t *testing.T, origEnv []string) {
	os.Clearenv()
	for _, pair := range origEnv {
		i := strings.Index(pair[1:], "=") + 1
		if err := os.Setenv(pair[:i], pair[i+1:]); err != nil {
			t.Errorf("Setenv(%q, %q) failed during reset: %v", pair[:i], pair[i+1:], err)
		}
	}
}
