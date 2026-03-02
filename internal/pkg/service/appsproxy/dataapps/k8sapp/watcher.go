package k8sapp

import (
	"context"
	"encoding/json"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	k8stypes "k8s.io/apimachinery/pkg/types"
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
}

// StateWatcher watches App CRDs in Kubernetes and provides a local cache of app states.
type StateWatcher struct {
	client    dynamic.Interface
	namespace string
	logger    log.Logger
	// byName: K8s object name → AppID
	byName sync.Map
	// byAppID: AppID → entry{k8sName, state}
	byAppID sync.Map
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
	}

	ctx, cancel := context.WithCancel(context.Background())
	d.Process().OnShutdown(func(context.Context) {
		cancel()
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

	go informer.Run(ctx.Done())

	// Log when the cache has synced so operators know the watcher is ready.
	go func() {
		if cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
			w.logger.Infof(ctx, "App CRD cache synced for namespace %q", namespace)
		}
	}()

	return w
}

// GetState returns the cached AppInfo for the app. Returns (AppInfo{}, false) if not yet cached.
func (w *StateWatcher) GetState(appID api.AppID) (AppInfo, bool) {
	v, ok := w.byAppID.Load(appID)
	if !ok {
		return AppInfo{}, false
	}
	e := v.(entry)
	return AppInfo{ActualState: e.state, AutoRestartEnabled: e.autoRestartEnabled}, true
}

// SetDesiredRunning patches .spec.state = "Running" on the App CRD for the given appID.
// If the appID is not yet in the cache, no patch is sent.
func (w *StateWatcher) SetDesiredRunning(ctx context.Context, appID api.AppID) error {
	v, ok := w.byAppID.Load(appID)
	if !ok {
		return nil
	}
	e := v.(entry)

	patch, err := json.Marshal(map[string]any{
		"spec": map[string]any{
			"state": "Running",
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

	data, err := u.MarshalJSON()
	if err != nil {
		w.logger.Errorf(ctx, "failed to marshal App CRD %q: %s", k8sName, err)
		return
	}

	var appObj appObject
	if err = json.Unmarshal(data, &appObj); err != nil {
		w.logger.Errorf(ctx, "failed to unmarshal App CRD %q: %s", k8sName, err)
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

	appID := api.AppID(appObj.Spec.AppID)
	w.byName.Store(k8sName, appID)
	w.byAppID.Store(appID, entry{k8sName: k8sName, state: appObj.Status.CurrentState, autoRestartEnabled: autoRestartEnabled})
}

func (w *StateWatcher) handleDelete(ctx context.Context, obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		// Handle tombstone objects from the cache.
		tombstone, ok2 := obj.(cache.DeletedFinalStateUnknown)
		if !ok2 {
			return
		}
		u, ok = tombstone.Obj.(*unstructured.Unstructured)
		if !ok {
			return
		}
	}
	k8sName := u.GetName()

	v, loaded := w.byName.LoadAndDelete(k8sName)
	if !loaded {
		return
	}
	appID := v.(api.AppID)
	w.byAppID.Delete(appID)
	w.logger.Debugf(ctx, "App CRD %q (appID=%s) removed from cache", k8sName, appID)
}
