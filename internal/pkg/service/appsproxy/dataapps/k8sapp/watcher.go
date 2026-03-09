package k8sapp

import (
	"context"
	"encoding/json"
	"net/url"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
)

// entry stores the K8s object name and last observed state for an app.
type entry struct {
	k8sName            string
	state              AppActualState
	autoRestartEnabled bool
	upstreamTarget     *url.URL // pre-parsed; nil when appsProxy.upstreamUrl absent/invalid
}

// StateWatcher watches App CRDs in Kubernetes and provides a local cache of app states.
type StateWatcher struct {
	client    dynamic.Interface
	namespace string
	logger    log.Logger
	hasSynced cache.InformerSynced
	// apps: AppID → entry
	apps sync.Map
}

type dependencies interface {
	Logger() log.Logger
	Process() *servicectx.Process
}

// NewDynamicClient creates a Kubernetes dynamic client from kubeconfig path or in-cluster config.
// If kubeconfigPath is empty, in-cluster config is used.
func NewDynamicClient(kubeconfigPath string) (dynamic.Interface, error) {
	var cfg *rest.Config
	var err error
	if kubeconfigPath != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, err
	}
	return dynamic.NewForConfig(cfg)
}

// NewStateWatcher creates and starts a StateWatcher that watches App CRDs in the given namespace.
// It registers the informer lifecycle with the process.
func NewStateWatcher(d dependencies, client dynamic.Interface, namespace string) *StateWatcher {
	w := &StateWatcher{
		client:    client,
		namespace: namespace,
		logger:    d.Logger().WithComponent("k8sapp.watcher"),
		hasSynced: func() bool { return false },
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	d.Process().OnShutdown(func(context.Context) {
		cancel(nil)
	})

	lw := &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			return client.Resource(AppGVR).Namespace(namespace).List(ctx, opts)
		},
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			return client.Resource(AppGVR).Namespace(namespace).Watch(ctx, opts)
		},
	}

	informer := cache.NewSharedIndexInformer(
		lw,
		&unstructured.Unstructured{},
		// No resync — rely on watch events only.
		0,
		cache.Indexers{},
	)

	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			w.handleUpsert(ctx, obj)
		},
		UpdateFunc: func(_, newObj any) {
			w.handleUpsert(ctx, newObj)
		},
		DeleteFunc: func(obj any) {
			w.handleDelete(ctx, obj)
		},
	})
	if err != nil {
		w.logger.Errorf(ctx, "failed to add event handler to App informer: %s", err)
	}

	w.hasSynced = informer.HasSynced

	go informer.Run(ctx.Done())

	return w
}

// GetState returns the cached AppInfo for the app. Returns (AppInfo{}, false) if not yet cached.
func (w *StateWatcher) GetState(appID api.AppID) (AppInfo, bool) {
	v, ok := w.apps.Load(appID)
	if !ok {
		return AppInfo{}, false
	}
	e := v.(entry)
	return AppInfo{
		ActualState:        e.state,
		AutoRestartEnabled: e.autoRestartEnabled,
		UpstreamTarget:     e.upstreamTarget,
	}, true
}

// WaitForCacheSync blocks until the informer cache has completed its initial list,
// the stopCh is closed, or the context is cancelled.
// Intended for use in tests to ensure the watch is established before creating objects.
func (w *StateWatcher) WaitForCacheSync(ctx context.Context) bool {
	return cache.WaitForCacheSync(ctx.Done(), w.hasSynced)
}

// WakeupApp patches spec.state = "Running" on the App CRD for the given appID.
// If the appID is not yet in the cache, no patch is sent.
func (w *StateWatcher) WakeupApp(ctx context.Context, appID api.AppID) error {
	v, ok := w.apps.Load(appID)
	if !ok {
		return nil
	}
	e := v.(entry)

	patch, err := json.Marshal(map[string]any{
		"spec": map[string]any{
			"state": AppActualStateRunning,
		},
	})
	if err != nil {
		return err
	}

	_, err = w.client.Resource(AppGVR).Namespace(w.namespace).Patch(
		ctx,
		e.k8sName,
		k8stypes.MergePatchType,
		patch,
		metav1.PatchOptions{},
	)
	return err
}

func (w *StateWatcher) handleUpsert(ctx context.Context, obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	k8sName := u.GetName()

	var appObj appObject
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &appObj); err != nil {
		w.logger.Errorf(ctx, "failed to convert App CRD %q: %s", k8sName, err)
		return
	}

	if appObj.Spec.AppID == "" {
		w.logger.Warnf(ctx, "App CRD %q has empty spec.appId, skipping", k8sName)
		return
	}

	autoRestartEnabled := true
	if appObj.Spec.AutoRestartEnabled != nil {
		autoRestartEnabled = *appObj.Spec.AutoRestartEnabled
	}

	var upstreamTarget *url.URL
	if rawURL := appObj.Status.AppsProxy.UpstreamURL; rawURL != "" {
		if t, err := url.Parse(rawURL); err == nil {
			upstreamTarget = t
		} else {
			w.logger.Warnf(ctx, "App CRD %q (appID=%s) invalid upstream URL %q from appsProxy.upstreamUrl: %s", k8sName, appObj.Spec.AppID, rawURL, err)
		}
	}

	appID := api.AppID(appObj.Spec.AppID)
	w.apps.Store(appID, entry{
		k8sName:            k8sName,
		state:              appObj.Status.CurrentState,
		autoRestartEnabled: autoRestartEnabled,
		upstreamTarget:     upstreamTarget,
	})
	w.logger.Debugf(ctx, "App CRD %q (appID=%s) state updated: actualState=%q autoRestartEnabled=%v upstreamTarget=%v", k8sName, appID, appObj.Status.CurrentState, autoRestartEnabled, upstreamTarget != nil)
}

func (w *StateWatcher) handleDelete(ctx context.Context, obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		// Handle tombstone objects from the cache.
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		u, ok = tombstone.Obj.(*unstructured.Unstructured)
		if !ok {
			return
		}
	}
	k8sName := u.GetName()

	w.apps.Range(func(key, val any) bool {
		if val.(entry).k8sName == k8sName {
			w.apps.Delete(key)
			w.logger.Debugf(ctx, "App CRD %q (appID=%s) removed from cache", k8sName, key)
			return false
		}
		return true
	})
}
