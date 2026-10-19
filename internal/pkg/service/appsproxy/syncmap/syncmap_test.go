package syncmap_test

import (
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	accessCounter := atomic.NewInt64(0)

	synctest.Test(t, func(t *testing.T) {
		// Launch 10 concurrent goroutines within the bubble
		for range 10 {
			go func() {
				m.GetOrInit("test")
				accessCounter.Add(1)
			}()
		}

		// Wait for all goroutines to be blocked
		synctest.Wait()
	})

	// Check total init count
	assert.Equal(t, int64(1), initCounter.Load())

	// Check total requests count
	assert.Equal(t, int64(10), accessCounter.Load())
}

// Without Delete a map keyed by a short-lived identity grows for the life of
// the process, because GetOrInit is the only way in.
func TestSyncMap_Delete(t *testing.T) {
	t.Parallel()

	inits := 0
	m := syncmap.New[string, int](func(string) *int {
		inits++
		v := inits
		return &v
	})

	first := m.GetOrInit("a")
	require.Same(t, first, m.GetOrInit("a"), "same key must return the same item")

	removed, ok := m.Delete("a")
	assert.True(t, ok)
	assert.Same(t, first, removed, "Delete returns the item it removed, so the caller can release it")

	assert.NotSame(t, first, m.GetOrInit("a"), "after Delete the key is re-initialised")

	_, ok = m.Delete("never-there")
	assert.False(t, ok)
}

func TestSyncMap_RangeWalksEveryItem(t *testing.T) {
	t.Parallel()

	m := syncmap.New[string, int](func(string) *int { return new(int) })
	*m.GetOrInit("a") = 1
	*m.GetOrInit("b") = 2

	got := map[string]int{}
	m.Range(func(k string, v *int) { got[k] = *v })

	assert.Equal(t, map[string]int{"a": 1, "b": 2}, got)
}

func TestSyncMap_RangeSkipsADeletedItem(t *testing.T) {
	t.Parallel()

	m := syncmap.New[string, int](func(string) *int { return new(int) })
	m.GetOrInit("a")
	m.GetOrInit("b")
	m.Delete("a")

	var keys []string
	m.Range(func(k string, _ *int) { keys = append(keys, k) })

	assert.Equal(t, []string{"b"}, keys)
}

// The idle timer sweeps this map once a second and the request path reads it on
// every request, so the walk must not hold the lock while fn runs.
func TestSyncMap_RangeDoesNotBlockGetOrInit(t *testing.T) {
	t.Parallel()

	m := syncmap.New[string, int](func(string) *int { return new(int) })
	m.GetOrInit("a")

	inRange := make(chan struct{})
	release := make(chan struct{})
	walked := make(chan struct{})
	go func() {
		defer close(walked)
		m.Range(func(string, *int) {
			close(inRange)
			<-release
		})
	}()
	<-inRange

	m.GetOrInit("b")

	close(release)
	<-walked
}
