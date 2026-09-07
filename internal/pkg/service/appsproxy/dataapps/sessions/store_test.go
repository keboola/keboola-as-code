package sessions

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStore_GetOrInit(t *testing.T) {
	t.Parallel()

	now := time.Now()
	s := newStore()

	item, created := s.getOrInit("a", now)
	assert.True(t, created)

	same, created := s.getOrInit("a", now.Add(time.Minute))
	assert.False(t, created)
	assert.Same(t, item, same)
	assert.Equal(t, now.Add(time.Minute), same.lastSeen)

	_, created = s.getOrInit("b", now)
	assert.True(t, created)
	assert.Equal(t, 2, s.len())
}

func TestStore_EvictBefore(t *testing.T) {
	t.Parallel()

	now := time.Now()
	s := newStore()

	s.getOrInit("idle", now.Add(-time.Hour))
	s.getOrInit("active", now)

	// Without eviction the map would grow with every visitor for the lifetime
	// of the process.
	assert.Equal(t, 1, s.evictBefore(now.Add(-30*time.Minute)))
	assert.Equal(t, 1, s.len())

	_, created := s.getOrInit("active", now)
	assert.False(t, created, "the active session must have been kept")

	_, created = s.getOrInit("idle", now)
	assert.True(t, created, "the idle session must have been evicted")
}

func TestStore_Take(t *testing.T) {
	t.Parallel()

	now := time.Now()
	s := newStore()
	item, _ := s.getOrInit("a", now)

	taken, ok := s.take("a")
	assert.True(t, ok)
	assert.Same(t, item, taken)
	assert.Equal(t, 0, s.len())

	// Taking again reports nothing, which is what makes a repeated End a no-op.
	_, ok = s.take("a")
	assert.False(t, ok)

	// And the session can start again from scratch, so activity after a
	// websocket reconnect is not silently dropped.
	_, created := s.getOrInit("a", now)
	assert.True(t, created)
}
