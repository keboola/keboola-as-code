package utils

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSafeCounter(t *testing.T) {
	c := NewSafeCounter(0)
	wg := sync.WaitGroup{}

	// Increment in goroutines
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			c.Inc()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			c.Inc()
		}
	}()

	// Wait for goroutines and assert
	wg.Wait()
	assert.Equal(t, 30, c.Get())
}
