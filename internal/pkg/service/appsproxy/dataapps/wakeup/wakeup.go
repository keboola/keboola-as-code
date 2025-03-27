// Package wakeup sends wakeup requests for an app, if the proxy received a request for the app, but the app does not run.
// The first request is sent immediately, the next after the Interval, otherwise the request is skipped.
package wakeup

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/syncmap"
)

// Interval sets how often the proxy sends wakeup request to sandboxes service.
// If the last notification for the app was less than this Interval ago then the notification is skipped.
const (
	Interval = time.Second
)

type Manager struct {
	clock     clockwork.Clock
	logger    log.Logger
	k8sClient dynamic.Interface
	stateMap  *syncmap.SyncMap[api.AppID, state]
}

type state struct {
	lock             *sync.Mutex
	nextRequestAfter time.Time
}

type dependencies interface {
	Clock() clockwork.Clock
	Logger() log.Logger
	K8sClient() dynamic.Interface
}

func NewManager(d dependencies) *Manager {
	return &Manager{
		clock:     d.Clock(),
		logger:    d.Logger(),
		k8sClient: d.K8sClient(),
		stateMap: syncmap.New[api.AppID, state](func(api.AppID) *state {
			return &state{lock: &sync.Mutex{}}
		}),
	}
}

// patchAppStatus creates a patch to update the App status to Running
func (l *Manager) patchAppStatus(ctx context.Context, appID api.AppID) error {
	// Create a patch that only updates the status.state field
	patch := map[string]interface{}{
		"spec": map[string]interface{}{
			"state": "Running",
		},
	}

	jsonData, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("failed to marshal patch: %w", err)
	}

	gvr := schema.GroupVersionResource{
		Group:    "apps.keboola.com",
		Version:  "v1",
		Resource: "apps",
	}

	// Create a namespaced name for the App resource
	nn := types.NamespacedName{
		Namespace: "sandbox",
		Name:      fmt.Sprintf("app-%s", appID),
	}

	// Apply the patch using dynamic client
	_, err = l.k8sClient.Resource(gvr).Namespace(nn.Namespace).Patch(
		ctx,
		nn.Name,
		types.MergePatchType,
		jsonData,
		v1.PatchOptions{},
	)
	if err != nil {
		return fmt.Errorf("failed to patch app status: %w", err)
	}

	return nil
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
	l.logger.Infof(ctx, `sending wakeup request for app "%s"`, appID)

	// Create patch for the App CRD
	err := l.patchAppStatus(ctx, appID)
	if err != nil {
		l.logger.Errorf(ctx, `failed to create patch for app "%s": %s`, appID, err.Error())
		return err
	}

	return nil
}
