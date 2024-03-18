package syncmap_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/syncmap"
)

type testStruct struct{}

func TestSyncMap(t *testing.T) {
	t.Parallel()

	m := syncmap.NewSyncMap[string, testStruct](func() *testStruct {
		return &testStruct{}
	})

	wg := sync.WaitGroup{}
	counter := atomic.NewInt64(0)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			m.GetOrInit("test")

			counter.Add(1)
		}()
	}

	// Wait for all requests
	wg.Wait()

	// Check total requests count
	assert.Equal(t, int64(10), counter.Load())
}
