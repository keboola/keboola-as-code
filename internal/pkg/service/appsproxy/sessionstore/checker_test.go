package sessionstore

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jonboulle/clockwork"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type checkerHarness struct {
	mr      *miniredis.Miniredis
	client  *redis.Client
	clock   *clockwork.FakeClock
	logger  log.DebugLogger
	checker *Checker
}

func newCheckerHarness(t *testing.T) *checkerHarness {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1, DialTimeout: 200 * time.Millisecond})
	t.Cleanup(func() { _ = client.Close() })
	clock := clockwork.NewFakeClock()
	logger := log.NewDebugLogger()
	return &checkerHarness{mr: mr, client: client, clock: clock, logger: logger, checker: NewChecker(logger, clock, client)}
}

// tick advances the fake clock by one interval, once the checker waits on its ticker.
func (h *checkerHarness) tick(t *testing.T) {
	t.Helper()
	require.NoError(t, h.clock.BlockUntilContext(t.Context(), 1))
	h.clock.Advance(checkInterval)
}

func (h *checkerHarness) waitUp(t *testing.T, up bool) {
	t.Helper()
	require.Eventually(t, func() bool { return h.checker.Up() == up }, 5*time.Second, 10*time.Millisecond)
}

func countLines(messages, substr string) int {
	n := 0
	for _, line := range strings.Split(messages, "\n") {
		if strings.Contains(line, substr) {
			n++
		}
	}
	return n
}

func TestChecker_StartupWithStoreDown_DoesNotBlock(t *testing.T) {
	t.Parallel()
	h := newCheckerHarness(t)
	h.mr.Close()

	start := time.Now()
	h.checker.Start(t.Context())
	assert.Less(t, time.Since(start), 100*time.Millisecond)
	assert.False(t, h.checker.Up())

	require.Eventually(t, func() bool {
		return countLines(h.logger.WarnMessages(), "session store is down") == 1
	}, 5*time.Second, 10*time.Millisecond)
	assert.False(t, h.checker.Up())
}

func TestChecker_Recovers(t *testing.T) {
	t.Parallel()
	h := newCheckerHarness(t)
	h.mr.Close()
	h.checker.Start(t.Context())
	require.Eventually(t, func() bool { return strings.Contains(h.logger.WarnMessages(), "session store is down") }, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, h.mr.Restart())
	h.tick(t)
	h.waitUp(t, true)
	assert.Equal(t, 1, countLines(h.logger.InfoMessages(), "session store is up"))
}

func TestChecker_UpAtStartup(t *testing.T) {
	t.Parallel()
	h := newCheckerHarness(t)
	h.checker.Start(t.Context())
	h.waitUp(t, true)
	assert.Empty(t, h.logger.WarnMessages())
}

func TestChecker_LogsOnlyStateChanges(t *testing.T) {
	t.Parallel()
	h := newCheckerHarness(t)
	h.checker.Start(t.Context())
	h.waitUp(t, true)

	h.mr.Close()
	for range 5 {
		h.tick(t)
	}
	h.waitUp(t, false)
	require.NoError(t, h.clock.BlockUntilContext(t.Context(), 1))
	assert.Equal(t, 1, countLines(h.logger.WarnMessages(), "session store is down"))
	assert.Equal(t, 1, countLines(h.logger.InfoMessages(), "session store is up"))
}

func TestChecker_StopsWithContext(t *testing.T) {
	t.Parallel()
	h := newCheckerHarness(t)
	ctx, cancel := context.WithCancelCause(t.Context())
	h.checker.Start(ctx)
	h.waitUp(t, true)
	require.NoError(t, h.clock.BlockUntilContext(t.Context(), 1))

	cancel(errors.New("test done"))
	require.Eventually(t, func() bool {
		return h.clock.BlockUntilContext(context.Background(), 0) == nil
	}, 5*time.Second, 10*time.Millisecond)

	h.mr.Close()
	h.clock.Advance(checkInterval)
	time.Sleep(100 * time.Millisecond)
	assert.True(t, h.checker.Up(), "no ping after the context is done")
}
