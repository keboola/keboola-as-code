package k8sapp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/url"
	"sync"

	"golang.org/x/sync/singleflight"
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
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// entry stores the K8s object name and last observed state for an app.
type entry struct {
	k8sName            string
	state              AppActualState
	autoRestartEnabled bool
	upstreamTarget     *url.URL // pre-parsed; nil when appsProxy.upstreamUrl absent/invalid
	e2bAccessToken     string   // loaded from K8s Secret; empty for non-E2B apps
	e2bSecretName      string   // Secret name for lazy token loading; empty for non-E2B apps
}

// StateWatcher watches App CRDs in Kubernetes and provides a local cache of app states.
type StateWatcher struct {
	client         dynamic.Interface
	namespace      string
	logger         log.Logger
	hasSynced      cache.InformerSynced
	apps           sync.Map           // AppID → entry
	tokenLoadGroup singleflight.Group // coalesces concurrent lazy-load K8s API calls per secret
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
	ctx, cancel := context.WithCancelCause(context.Background())
	d.Process().OnShutdown(func(context.Context) {
		cancel(nil)
	})

	lw := &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
			return client.Resource(AppGVR()).Namespace(namespace).List(ctx, opts)
		},
		WatchFuncWithContext: func(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
			return client.Resource(AppGVR()).Namespace(namespace).Watch(ctx, opts)
		},
	}

	informer := cache.NewSharedIndexInformer(
		lw,
		&unstructured.Unstructured{},
		0, // No resync — rely on watch events only.
		cache.Indexers{},
	)

	w := &StateWatcher{
		client:    client,
		namespace: namespace,
		logger:    d.Logger().WithComponent("k8sapp.watcher"),
		hasSynced: informer.HasSynced,
	}

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
	// AddEventHandler only errors if the informer is already stopped.
	// Since Run has not been called yet, this error is unreachable here.
	if err != nil {
		w.logger.Errorf(ctx, "failed to add event handler to App informer: %s", err)
	}

	go informer.Run(ctx.Done())

	return w
}

// GetState returns the cached AppInfo for the app. Returns (AppInfo{}, false) if not yet cached.
// If the E2B access token is missing but a secret name is known, it attempts to load the token lazily.
func (w *StateWatcher) GetState(appID api.AppID) (AppInfo, bool) {
	v, ok := w.apps.Load(appID)
	if !ok {
		return AppInfo{}, false
	}
	e := v.(entry)

	// Lazy-load E2B token: the Secret may not have existed when the App CRD event was processed.
	// a singleflight coalesces concurrent requests for the same secret into a single K8s API call.
	if e.e2bAccessToken == "" && e.e2bSecretName != "" {
		token, err, _ := w.tokenLoadGroup.Do(e.e2bSecretName, func() (any, error) {
			return w.loadSecretToken(context.Background(), e.e2bSecretName)
		})
		if err != nil {
			w.logger.Warnf(context.Background(), "App %s: failed to lazy-load E2B access token from secret %q: %s", appID, e.e2bSecretName, err)
		} else if t, ok := token.(string); t != "" && ok {
			e.e2bAccessToken = t
			w.apps.Store(appID, e)
			w.logger.Infof(context.Background(), "App %s: lazy-loaded E2B access token from secret %q", appID, e.e2bSecretName)
		}
	}

	return AppInfo{
		ActualState:        e.state,
		AutoRestartEnabled: e.autoRestartEnabled,
		UpstreamTarget:     e.upstreamTarget,
		E2BAccessToken:     e.e2bAccessToken,
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

	_, err = w.client.Resource(AppGVR()).Namespace(w.namespace).Patch(
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

	var e2bAccessToken string
	var e2bSecretName string
	if appObj.Spec.Runtime.Backend.Type == BackendTypeE2BSandbox {
		e2bSecretName = appObj.Status.E2BSandbox.AccessTokenSecretName
		if e2bSecretName != "" {
			token, err := w.loadSecretToken(ctx, e2bSecretName)
			if err == nil {
				e2bAccessToken = token
			}
		}
	}

	appID := api.AppID(appObj.Spec.AppID)
	w.apps.Store(appID, entry{
		k8sName:            k8sName,
		state:              appObj.Status.CurrentState,
		autoRestartEnabled: autoRestartEnabled,
		upstreamTarget:     upstreamTarget,
		e2bAccessToken:     e2bAccessToken,
		e2bSecretName:      e2bSecretName,
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
		e, ok := val.(entry)
		if ok && e.k8sName == k8sName {
			w.apps.Delete(key)
			w.logger.Debugf(ctx, "App CRD %q (appID=%s) removed from cache", k8sName, key)
			return false
		}
		return true
	})
}

// loadSecretToken fetches a K8s Secret by name and returns the value of the "token" key.
// The dynamic client returns Secret data values as base64-encoded strings.
func (w *StateWatcher) loadSecretToken(ctx context.Context, secretName string) (string, error) {
	obj, err := w.client.Resource(SecretGVR()).Namespace(w.namespace).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}

	data, found, err := unstructured.NestedMap(obj.Object, "data")
	if err != nil {
		return "", errors.Errorf("secret %q: failed to read data field: %s", secretName, err)
	}
	if !found {
		return "", errors.Errorf("secret %q has no data field", secretName)
	}

	token, ok := data["token"].(string)
	if !ok || token == "" {
		return "", errors.Errorf("secret %q has no \"token\" key in data", secretName)
	}

	tokenBytes, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return "", errors.Errorf("secret %q: failed to base64-decode token: %s", secretName, err)
	}

	return string(tokenBytes), nil
}
