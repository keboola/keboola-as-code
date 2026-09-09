package sessions

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
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

func TestManager_RetentionCoversTheLongestCookie(t *testing.T) {
	t.Parallel()

	// An entry has to outlive the longest deadline a cookie can be granted, not
	// just the idle window. A websocket handshake buys wsTimeout +
	// websocketGrace, and lastSeen moves only on a data frame — ping and pong
	// are control frames and deliberately do not count. Retaining for the idle
	// window alone would drop the entry of a live but quiet connection, and the
	// close that follows would find nothing: no event, counters gone.
	m := &Manager{
		cfg:       config.Sessions{IdleTimeout: 30 * time.Minute},
		wsTimeout: 6 * time.Hour,
	}
	assert.Equal(t, 6*time.Hour+websocketGrace, m.retention())

	// A stack that gives websockets a shorter life than the idle window still
	// keeps entries for the whole idle window.
	m = &Manager{
		cfg:       config.Sessions{IdleTimeout: 12 * time.Hour},
		wsTimeout: time.Minute,
	}
	assert.Equal(t, 12*time.Hour, m.retention())
}
