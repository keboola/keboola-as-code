package sessionstore

import (
	"context"
	"strings"
	"sync/atomic"
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
	pings   *atomic.Int64
}

// countPings counts the pings that have returned, so a test can wait until a check has run.
type countPings struct {
	pings *atomic.Int64
}

func (h countPings) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h countPings) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if cmd.Name() == "ping" {
			h.pings.Add(1)
		}
		return err
	}
}

func (h countPings) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func newCheckerHarness(t *testing.T) *checkerHarness {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1, DialTimeout: 200 * time.Millisecond})
	t.Cleanup(func() { _ = client.Close() })
	pings := &atomic.Int64{}
	client.AddHook(countPings{pings: pings})
	clock := clockwork.NewFakeClock()
	logger := log.NewDebugLogger()
	return &checkerHarness{mr: mr, client: client, clock: clock, logger: logger, checker: NewChecker(logger, clock, client), pings: pings}
}

// tick advances the fake clock by one interval and waits until the check it triggers has pinged.
// The ticker counts as a waiter from its creation, so BlockUntil alone does not wait for a check.
func (h *checkerHarness) tick(t *testing.T) {
	t.Helper()
	before := h.pings.Load()
	require.NoError(t, h.clock.BlockUntilContext(t.Context(), 1))
	h.clock.Advance(checkInterval)
	require.Eventually(t, func() bool { return h.pings.Load() > before }, 5*time.Second, 10*time.Millisecond)
}

// waitLoggedOnce waits until the messages contain the line exactly once; the state is stored before it is logged.
func waitLoggedOnce(t *testing.T, messages func() string, substr string) {
	t.Helper()
	require.Eventually(t, func() bool { return countLines(messages(), substr) == 1 }, 5*time.Second, 10*time.Millisecond,
		"expected one %q line in %s", substr, messages())
}

func (h *checkerHarness) waitUp(t *testing.T) {
	t.Helper()
	require.Eventually(t, h.checker.Up, 5*time.Second, 10*time.Millisecond)
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
	h.waitUp(t)
	waitLoggedOnce(t, h.logger.InfoMessages, "session store is up")
}

func TestChecker_UpAtStartup(t *testing.T) {
	t.Parallel()
	h := newCheckerHarness(t)
	h.checker.Start(t.Context())
	h.waitUp(t)
	assert.Empty(t, h.logger.WarnMessages())
}

func TestChecker_LogsOnlyStateChanges(t *testing.T) {
	t.Parallel()
	h := newCheckerHarness(t)
	h.checker.Start(t.Context())
	h.waitUp(t)
	waitLoggedOnce(t, h.logger.InfoMessages, "session store is up")

	h.mr.Close()
	for range 5 {
		h.tick(t)
	}
	// One more check: the checks run one after another, so the five before it have logged.
	h.tick(t)
	assert.False(t, h.checker.Up())
	assert.Equal(t, 1, countLines(h.logger.WarnMessages(), "session store is down"))
	assert.Equal(t, 1, countLines(h.logger.InfoMessages(), "session store is up"))
}

func TestChecker_StopsWithContext(t *testing.T) {
	t.Parallel()
	h := newCheckerHarness(t)
	ctx, cancel := context.WithCancelCause(t.Context())
	h.checker.Start(ctx)
	h.waitUp(t)
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
