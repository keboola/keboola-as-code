package

// Package notify provides notifications that the app is actively used. This prevents the app from sleeping.
// The first notification is sent immediately, the next after the interval, otherwise the notification is skipped.
notify

import (
	"context"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/sasha-s/go-deadlock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/syncmap"
)

// Interval sets how often the proxy sends notifications to sandboxes service.
// If the last notification for the app was less than this interval ago then the notification is skipped.
const Interval = time.Second * 30

type Manager struct {
	clock    clockwork.Clock
	logger   log.Logger
	api      *api.API
	stateMap *syncmap.SyncMap[api.AppID, state]
}

type state struct {
	lock                  *deadlock.Mutex
	nextNotificationAfter time.Time
}

type dependencies interface {
	Clock() clockwork.Clock
	Logger() log.Logger
	AppsAPI() *api.API
}

func NewManager(d dependencies) *Manager {
	return &Manager{
		clock:  d.Clock(),
		logger: d.Logger(),
		api:    d.AppsAPI(),
		stateMap: syncmap.New[api.AppID, state](func(api.AppID) *state {
			return &state{lock: &deadlock.Mutex{}}
		}),
	}
}

func (l *Manager) Notify(ctx context.Context, appID api.AppID) error {
	// Get cache item or init an empty item
	item := l.stateMap.GetOrInit(appID)

	// Only one notification runs in parallel.
	// If there is an in-flight update, we are waiting for its results.
	item.lock.Lock()
	defer item.lock.Unlock()

	// Return config from cache if still valid
	now := l.clock.Now()

	if now.Before(item.nextNotificationAfter) {
		// Skip if a notification was sent less than interval ago
		return nil
	}

	// Update nextNotificationAfter time
	item.nextNotificationAfter = now.Add(Interval)

	// Send the notification
	if _, err := l.api.NotifyAppUsage(appID, now).Send(ctx); err != nil {
		l.logger.Errorf(ctx, `failed notifying Sandboxes Service about a request to app "%s": %s`, appID, err.Error())
		return err
	}

	return nil
}
