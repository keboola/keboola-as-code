// Package wakeup sends wakeup requests for an app, if the proxy received a request for the app, but the app does not run.
// The first request is sent immediately, the next after the Interval, otherwise the request is skipped.
package wakeup

import (
	"context"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/syncmap"
)

// Interval sets how often the proxy sends a wakeup request.
// If the last wakeup for the app was less than this Interval ago then the wakeup is skipped.
const Interval = time.Second

type Manager struct {
	clock    clockwork.Clock
	logger   log.Logger
	watcher  *k8sapp.StateWatcher
	stateMap *syncmap.SyncMap[api.AppID, state]
}

type state struct {
	lock             sync.Mutex
	nextRequestAfter time.Time
}

type dependencies interface {
	Clock() clockwork.Clock
	Logger() log.Logger
	AppStateWatcher() *k8sapp.StateWatcher
}

func NewManager(d dependencies) *Manager {
	return &Manager{
		clock:   d.Clock(),
		logger:  d.Logger(),
		watcher: d.AppStateWatcher(),
		stateMap: syncmap.New[api.AppID, state](func(api.AppID) *state {
			return &state{}
		}),
	}
}

func (l *Manager) Wakeup(ctx context.Context, appID api.AppID) error {
	item := l.stateMap.GetOrInit(appID)

	// Serialize per-app wakeups to make the rate-limit check atomic.
	item.lock.Lock()
	defer item.lock.Unlock()

	now := l.clock.Now()
	if now.Before(item.nextRequestAfter) {
		return nil
	}

	item.nextRequestAfter = now.Add(Interval)

	if err := l.watcher.SetDesiredRunning(ctx, appID); err != nil {
		l.logger.Errorf(ctx, `failed setting desired state "Running" for app "%s": %s`, appID, err)
		return err
	}
	return nil
}
