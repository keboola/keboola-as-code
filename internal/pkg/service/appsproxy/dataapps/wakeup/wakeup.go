// Package wakeup sends wakeup requests for an app, if the proxy received a request for the app, but the app does not run.
// The first request is sent immediately, the next after the Interval, otherwise the request is skipped.
package wakeup

import (
	"context"
	"sync"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/syncmap"
)

// Interval sets how often the proxy sends wakeup request to sandboxes service.
// If the last notification for the app was less than this Interval ago then the notification is skipped.
const Interval = time.Second

type Manager struct {
	clock    clock.Clock
	logger   log.Logger
	api      *api.API
	stateMap *syncmap.SyncMap[api.AppID, state]
}

type state struct {
	lock             *sync.Mutex
	nextRequestAfter time.Time
}

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	AppsAPI() *api.API
}

func NewManager(d dependencies) *Manager {
	return &Manager{
		clock:  d.Clock(),
		logger: d.Logger(),
		api:    d.AppsAPI(),
		stateMap: syncmap.New[api.AppID, state](func(api.AppID) *state {
			return &state{lock: &sync.Mutex{}}
		}),
	}
}

func (l *Manager) Wakeup(ctx context.Context, appID api.AppID) error {
	// Get cache item or init an empty item
	item := l.stateMap.GetOrInit(appID)

	// Only one notification runs in parallel.
	// If there is an in-flight update, we are waiting for its results.
	item.lock.Lock()
	defer item.lock.Unlock()

	// Return config from cache if still valid
	now := l.clock.Now()

	if now.Before(item.nextRequestAfter) {
		// Skip if a notification was sent less than Interval ago
		return nil
	}

	// Update nextRequestAfter time
	item.nextRequestAfter = now.Add(Interval)

	// Send the notification
	if _, err := l.api.WakeupApp(appID).Send(ctx); err != nil {
		l.logger.Errorf(ctx, `failed sending wakeup request to Sandboxes Service about for app "%s": %s`, appID, err.Error())
		return err
	}
	return nil
}
