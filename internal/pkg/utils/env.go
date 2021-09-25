package utils

import (
	"fmt"
	"os"
)

func MustGetEnv(key string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		panic(fmt.Errorf("missing ENV variable \"%s\"", key))
	}
	return value
}

func MustSetEnv(key string, value string) {
	if err := os.Setenv(key, value); err != nil {
		panic(fmt.Errorf("cannot set env variable \"%s\": %w", key, err))
	}
}
