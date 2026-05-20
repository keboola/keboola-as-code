package kaipreview

import (
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testHandshakeKey = "test-handshake-key-must-be-long-enough"
	testSessionKey   = "test-session-key-also-long-enough"
)

func TestHandshakeJWT_RoundTrip(t *testing.T) {
	t.Parallel()
	clock := clockwork.NewFakeClock()

	token, err := MintHandshakeJWT(testHandshakeKey, clock, "app-123", "proj-456")
	require.NoError(t, err)
	require.NotEmpty(t, token)

	claims, err := VerifyHandshakeJWT(testHandshakeKey, clock, token)
	require.NoError(t, err)
	assert.Equal(t, 1, claims.Ver)
	assert.Equal(t, "app-123", claims.AppID)
	assert.Equal(t, "proj-456", claims.ProjectID)
	assert.Equal(t, "kai-preview-handshake", claims.Purpose)
	assert.NotEmpty(t, claims.ID)
}

func TestHandshakeJWT_ExpiredAfter60s(t *testing.T) {
	t.Parallel()
	clock := clockwork.NewFakeClock()

	token, err := MintHandshakeJWT(testHandshakeKey, clock, "app-123", "proj-456")
	require.NoError(t, err)

	clock.Advance(61 * time.Second)

	_, err = VerifyHandshakeJWT(testHandshakeKey, clock, token)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expired")
}

func TestHandshakeJWT_WrongKeyRejected(t *testing.T) {
	t.Parallel()
	clock := clockwork.NewFakeClock()

	token, err := MintHandshakeJWT(testHandshakeKey, clock, "app-123", "proj-456")
	require.NoError(t, err)

	_, err = VerifyHandshakeJWT("different-key-different-length", clock, token)
	require.Error(t, err)
}

func TestSessionJWT_RoundTrip(t *testing.T) {
	t.Parallel()
	clock := clockwork.NewFakeClock()

	token, err := MintSessionJWT(testSessionKey, clock, "app-123", "proj-456", 4*time.Hour)
	require.NoError(t, err)

	claims, err := VerifySessionJWT(testSessionKey, clock, token)
	require.NoError(t, err)
	assert.Equal(t, 1, claims.Ver)
	assert.Equal(t, "app-123", claims.AppID)
	assert.Equal(t, "proj-456", claims.ProjectID)
	assert.Equal(t, "kai-preview-session", claims.Purpose)
}

func TestSessionJWT_ExpiredAfterTTL(t *testing.T) {
	t.Parallel()
	clock := clockwork.NewFakeClock()

	token, err := MintSessionJWT(testSessionKey, clock, "app-123", "proj-456", 4*time.Hour)
	require.NoError(t, err)

	clock.Advance(4*time.Hour + time.Second)

	_, err = VerifySessionJWT(testSessionKey, clock, token)
	require.Error(t, err)
}

func TestSessionJWT_HalfwayDetection(t *testing.T) {
	t.Parallel()
	clock := clockwork.NewFakeClock()

	token, err := MintSessionJWT(testSessionKey, clock, "app-123", "proj-456", 4*time.Hour)
	require.NoError(t, err)

	clock.Advance(1 * time.Hour) // < 50% — should not need refresh
	claims, err := VerifySessionJWT(testSessionKey, clock, token)
	require.NoError(t, err)
	assert.False(t, claims.NeedsRefresh(clock.Now()))

	clock.Advance(2 * time.Hour) // total 3h, > 50% of 4h
	claims, err = VerifySessionJWT(testSessionKey, clock, token)
	require.NoError(t, err)
	assert.True(t, claims.NeedsRefresh(clock.Now()))
}
