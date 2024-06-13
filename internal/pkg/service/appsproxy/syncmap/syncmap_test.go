package syncmap_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/syncmap"
)

type testStruct struct{}

func TestSyncMap_GetOrInit(t *testing.T) {
	t.Parallel()

	m := syncmap.New[string, testStruct](func(string) *testStruct {
		return &testStruct{}
	})

	instance := m.GetOrInit("test")
	assert.Same(t, instance, m.GetOrInit("test"))
}

func TestSyncMap_GetOrInit_Race(t *testing.T) {
	t.Parallel()

	initCounter := atomic.NewInt64(0)
	m := syncmap.New[string, testStruct](func(string) *testStruct {
		initCounter.Add(1)
		return &testStruct{}
	})

	wg := sync.WaitGroup{}
	accessCounter := atomic.NewInt64(0)
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			m.GetOrInit("test")

			accessCounter.Add(1)
		}()
	}

	// Wait for all requests
	wg.Wait()

	// Check total init count
	assert.Equal(t, int64(1), initCounter.Load())

	// Check total requests count
	assert.Equal(t, int64(10), accessCounter.Load())
}
