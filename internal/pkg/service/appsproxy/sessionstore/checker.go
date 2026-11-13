package sessionstore

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/redis/go-redis/v9"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	checkInterval = 10 * time.Second
	pingTimeout   = 500 * time.Millisecond
)

// Checker pings the session store. Its state goes to logs and the sessionstore.up metric only, never to /health-check:
// a store outage must not take apps-proxy out of service for apps that do not use the store.
type Checker struct {
	logger  log.Logger
	clock   clockwork.Clock
	client  *redis.Client
	up      atomic.Bool
	checked bool
}

func NewChecker(logger log.Logger, clock clockwork.Clock, client *redis.Client) *Checker {
	return &Checker{logger: logger, clock: clock, client: client}
}

func (c *Checker) Up() bool {
	return c.up.Load()
}

// Start pings at once and then every checkInterval, in the background, until ctx is done.
func (c *Checker) Start(ctx context.Context) {
	go c.run(ctx)
}

func (c *Checker) run(ctx context.Context) {
	ticker := c.clock.NewTicker(checkInterval)
	defer ticker.Stop()

	c.check(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.Chan():
			c.check(ctx)
		}
	}
}

func (c *Checker) check(ctx context.Context) {
	pingCtx, cancel := context.WithTimeoutCause(ctx, pingTimeout, errors.New("session store ping timeout"))
	defer cancel()
	err := c.client.Ping(pingCtx).Err()
	if ctx.Err() != nil {
		return
	}

	up := err == nil
	changed := !c.checked || c.up.Load() != up
	c.checked = true
	c.up.Store(up)
	if !changed {
		return
	}
	if up {
		c.logger.Infof(ctx, "session store is up")
	} else {
		c.logger.Warnf(ctx, "session store is down: %s", err)
	}
}
