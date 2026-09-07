package sessions

import (
	"sync"
	"time"
)

// entry is the in-memory state of one session on one proxy replica.
//
// It is a cache, never a source of truth. The proxy runs several replicas and
// restarts on deploy, so the same session can be tracked by more than one
// entry over its lifetime, or by none. Everything that must survive lives in
// the cookie (session id, and the start time encoded in it) or in the emitted
// events. Consequences that downstream queries must tolerate:
//
//   - heartbeat deltas of one session may arrive from two replicas,
//   - session_end may never arrive at all.
type entry struct {
	lock               sync.Mutex
	lastSeen           time.Time
	nextHeartbeatAfter time.Time
	requests           int
	wsFrames           int
	identitySent       bool
}

// store holds session entries with time-based eviction.
//
// Eviction is not optional: without it the map grows with every visitor for as
// long as the process lives.
type store struct {
	lock  sync.RWMutex
	items map[string]*entry
}

func newStore() *store {
	return &store{items: make(map[string]*entry)}
}

// getOrInit returns the entry for the session, creating it when absent.
// The second return value reports whether it was created by this call.
func (s *store) getOrInit(sessionID string, now time.Time) (*entry, bool) {
	s.lock.RLock()
	if item, ok := s.items[sessionID]; ok {
		s.lock.RUnlock()
		item.lock.Lock()
		item.lastSeen = now
		item.lock.Unlock()
		return item, false
	}
	s.lock.RUnlock()

	s.lock.Lock()
	defer s.lock.Unlock()

	// Re-check: another goroutine may have created it while the lock was released.
	if item, ok := s.items[sessionID]; ok {
		item.lock.Lock()
		item.lastSeen = now
		item.lock.Unlock()
		return item, false
	}

	item := &entry{lastSeen: now}
	s.items[sessionID] = item
	return item, true
}

// take removes the entry and returns it, or (nil, false) when there is none.
//
// Ending a session removes its entry rather than flagging it, so that activity
// arriving afterwards re-initialises cleanly. A Streamlit websocket closes and
// reconnects routinely — on its ~20 min reconnect cycle, on a network blip, and
// at the 6 h upstream timeout — and the user keeps working through it. A
// terminal flag here would silence that session on this replica for good.
func (s *store) take(sessionID string) (*entry, bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	item, ok := s.items[sessionID]
	if !ok {
		return nil, false
	}
	delete(s.items, sessionID)
	return item, true
}

// evictBefore drops entries not seen since the given time and returns how many
// were removed.
func (s *store) evictBefore(cutoff time.Time) int {
	s.lock.Lock()
	defer s.lock.Unlock()

	removed := 0
	for id, item := range s.items {
		item.lock.Lock()
		stale := item.lastSeen.Before(cutoff)
		item.lock.Unlock()
		if stale {
			delete(s.items, id)
			removed++
		}
	}
	return removed
}

func (s *store) len() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return len(s.items)
}
