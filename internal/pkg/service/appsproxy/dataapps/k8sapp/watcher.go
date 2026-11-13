package k8sapp

import (
	"context"
	"encoding/json"
	"net/url"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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

type entry struct {
	k8sName            string
	appID              api.AppID
	host               string // exact hostname from appsProxy.publicUrl; empty when none is published
	state              AppActualState
	autoRestartEnabled bool
	devMode            bool
	upstreamTarget     *url.URL // pre-parsed; nil when appsProxy.upstreamUrl absent/invalid
	workspaceID        string   // read on Sandbox entries only; an App resolves it through productionSandbox
	notDeployed        bool

	uid               k8stypes.UID   // metadata.uid, required by any ownerReference naming this object
	productionSandbox string         // App CRs only: the member Sandbox running the workload
	autoSuspendAfter  *time.Duration // Sandbox CRs only; nil means the CR carries no threshold
	lastStarted       time.Time      // Sandbox: when it started; App: when it entered Running or its route moved
}

// StateWatcher watches App and Sandbox CRDs in Kubernetes and provides a local
// cache of workload states.
type StateWatcher struct {
	client              dynamic.Interface
	namespace           string
	logger              log.Logger
	hasSynced           cache.InformerSynced
	sandboxesHaveSynced cache.InformerSynced
	apps                map[api.AppID]entry // guarded by routeLock

	// routeLock guards all three caches, for reads as well as writes.
	routeLock    sync.RWMutex
	sandboxes    map[string]entry  // Sandbox K8s object name → entry
	sandboxHosts map[string]string // exact hostname → Sandbox K8s object name

	workloadRemovalNotifier workloadRemovalNotifier
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
	return dynamic.NewForConfig(withRateLimits(cfg))
}

// withRateLimits replaces client-go's 5 QPS and burst 10, which are sized for a
// small controller. One client is shared by the informers, the wake path, the
// suspend path and every activity write, and a write that queues past its
// timeout leaves the shared idle record stale, which is how a load peak becomes
// an early suspend. The apiserver's own priority and fairness, not this, is
// what protects the cluster.
func withRateLimits(cfg *rest.Config) *rest.Config {
	cfg.QPS = 50
	cfg.Burst = 100
	return cfg
}

// NewStateWatcher creates and starts a StateWatcher that watches App and Sandbox CRDs in the given namespace.
// It registers the informer lifecycle with the process.
func NewStateWatcher(d dependencies, client dynamic.Interface, namespace string) *StateWatcher {
	ctx, cancel := context.WithCancelCause(context.Background())
	d.Process().OnShutdown(func(context.Context) {
		cancel(nil)
	})

	appInformer := NewCRDInformer(client, AppGVR(), namespace)
	sandboxInformer := NewCRDInformer(client, SandboxGVR(), namespace)

	w := &StateWatcher{
		client:              client,
		namespace:           namespace,
		logger:              d.Logger().WithComponent("k8sapp.watcher"),
		hasSynced:           appInformer.HasSynced,
		sandboxesHaveSynced: sandboxInformer.HasSynced,
		apps:                make(map[api.AppID]entry),
		sandboxes:           make(map[string]entry),
		sandboxHosts:        make(map[string]string),
	}

	w.addEventHandler(ctx, appInformer, "App", w.handleUpsert, w.handleDelete)
	w.addEventHandler(ctx, sandboxInformer, "Sandbox", w.handleSandboxUpsert, w.handleSandboxDelete)

	go appInformer.Run(ctx.Done())
	go sandboxInformer.Run(ctx.Done())

	return w
}

// NewCRDInformer builds a watch-backed cache of one CRD kind in one namespace.
func NewCRDInformer(client dynamic.Interface, gvr schema.GroupVersionResource, namespace string) cache.SharedIndexInformer {
	lw := &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
			return client.Resource(gvr).Namespace(namespace).List(ctx, opts)
		},
		WatchFuncWithContext: func(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
			return client.Resource(gvr).Namespace(namespace).Watch(ctx, opts)
		},
	}

	return cache.NewSharedIndexInformer(
		cache.ToListWatcherWithWatchListSemantics(lw, client),
		&unstructured.Unstructured{},
		0, // No resync — rely on watch events only.
		cache.Indexers{},
	)
}

func (w *StateWatcher) addEventHandler(ctx context.Context, informer cache.SharedIndexInformer, kind string, upsert, remove func(context.Context, any)) {
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			upsert(ctx, obj)
		},
		UpdateFunc: func(_, newObj any) {
			upsert(ctx, newObj)
		},
		DeleteFunc: func(obj any) {
			remove(ctx, obj)
		},
	})
	// AddEventHandler only errors if the informer is already stopped.
	// Since Run has not been called yet, this error is unreachable here.
	if err != nil {
		w.logger.Errorf(ctx, "failed to add event handler to %s informer: %s", kind, err)
	}
}

// GetState returns the cached AppInfo for the workload. Returns (AppInfo{}, false) if not yet cached.
func (w *StateWatcher) GetState(ref WorkloadRef) (AppInfo, bool) {
	e, ok := w.entryFor(ref)
	if !ok {
		return AppInfo{}, false
	}

	return AppInfo{
		ActualState:        e.state,
		PublicHost:         e.host,
		AutoRestartEnabled: e.autoRestartEnabled,
		DevMode:            e.devMode,
		UpstreamTarget:     e.upstreamTarget,
		WorkspaceID:        w.workspaceFor(ref, e),
		NotDeployed:        e.notDeployed,
	}, true
}

// HasSynced reports whether all informers have completed their initial list.
// Unlike WaitForCacheSync it does not block, so a periodic caller can skip a
// round rather than act on a cache that is still filling.
func (w *StateWatcher) HasSynced() bool {
	return synced(w.hasSynced) && synced(w.sandboxesHaveSynced)
}

func synced(fn cache.InformerSynced) bool {
	return fn != nil && fn()
}

// WaitForCacheSync blocks until the informer cache has completed its initial list,
// the stopCh is closed, or the context is cancelled.
// Intended for use in tests to ensure the watch is established before creating objects.
func (w *StateWatcher) WaitForCacheSync(ctx context.Context) bool {
	return cache.WaitForCacheSync(ctx.Done(), w.hasSynced, w.sandboxesHaveSynced)
}

// Wakeup patches spec.state = "Running" on the CRD of the workload that owns
// the route: the Sandbox CR when the route belongs to one, the App CR otherwise.
// A workload missing from the cache is an error: patching nothing must not be
// reported as a successful wake.
func (w *StateWatcher) Wakeup(ctx context.Context, ref WorkloadRef) error {
	e, ok := w.entryFor(ref)
	if !ok {
		return errors.Errorf("workload %q is not in the cache, nothing was woken", ref)
	}

	patch, err := json.Marshal(map[string]any{
		"spec": map[string]any{
			"state": AppActualStateRunning,
		},
	})
	if err != nil {
		return err
	}

	_, err = w.client.Resource(gvrFor(ref)).Namespace(w.namespace).Patch(
		ctx,
		e.k8sName,
		k8stypes.MergePatchType,
		patch,
		metav1.PatchOptions{},
	)
	return err
}

// OnWorkloadRemoved registers a callback fired when a workload leaves the cache.
func (w *StateWatcher) OnWorkloadRemoved(fn func(WorkloadRef)) {
	w.workloadRemovalNotifier.subscribe(fn)
}

// ResolveWorkloadForHost reports the workload that owns an exact hostname. A
// Sandbox that publishes status.appsProxy.publicUrl owns it, and its own
// spec.appId names the app config, so the hostname need not contain an app id.
//
// Nothing arbitrates between a Sandbox and an App: the platform allocates every
// published hostname and does not issue one to a Sandbox that an App already
// answers on. A workload kind with a free-form hostname breaks that assumption
// and has to be reconsidered here before one exists.
func (w *StateWatcher) ResolveWorkloadForHost(host string) (WorkloadRef, bool) {
	host = NormalizeHost(host)

	w.routeLock.RLock()
	defer w.routeLock.RUnlock()

	k8sName, claimed := w.sandboxHosts[host]
	if !claimed {
		return WorkloadRef{}, false
	}
	e, found := w.sandboxes[k8sName]
	if !found {
		return WorkloadRef{}, false
	}

	return WorkloadRef{AppID: e.appID, SandboxName: k8sName}, true
}

func (w *StateWatcher) entryFor(ref WorkloadRef) (entry, bool) {
	w.routeLock.RLock()
	defer w.routeLock.RUnlock()

	if ref.IsSandbox() {
		e, ok := w.sandboxes[ref.SandboxName]
		return e, ok
	}
	e, ok := w.apps[ref.AppID]
	return e, ok
}

// workspaceFor returns the workspace of the Sandbox that runs the workload: a
// draft's own, or the one the App names in status.productionSandbox.
func (w *StateWatcher) workspaceFor(ref WorkloadRef, e entry) string {
	if ref.IsSandbox() {
		return e.workspaceID
	}

	w.routeLock.RLock()
	defer w.routeLock.RUnlock()

	member, ok := w.productionMemberLocked(e)
	if !ok || member.appID != e.appID {
		return ""
	}
	return member.workspaceID
}

// productionMemberLocked returns the cached Sandbox the App names in
// status.productionSandbox. The caller must hold routeLock.
func (w *StateWatcher) productionMemberLocked(app entry) (entry, bool) {
	if app.productionSandbox == "" {
		return entry{}, false
	}
	member, ok := w.sandboxes[app.productionSandbox]
	return member, ok
}

func (w *StateWatcher) handleUpsert(ctx context.Context, obj any) {
	parsed, ok := w.parseObject(ctx, "App", obj)
	if !ok {
		return
	}

	appID := api.AppID(parsed.appID)

	w.putApp(appID, parsed.entry)

	w.logger.Debugf(ctx, "App CRD %q (appID=%s) state updated: actualState=%q autoRestartEnabled=%v devMode=%v upstreamTarget=%v", parsed.entry.k8sName, appID, parsed.entry.state, parsed.entry.autoRestartEnabled, parsed.entry.devMode, parsed.entry.upstreamTarget != nil)
}

func (w *StateWatcher) handleSandboxUpsert(ctx context.Context, obj any) {
	parsed, ok := w.parseObject(ctx, "Sandbox", obj)
	if !ok {
		return
	}

	w.storeSandbox(ctx, api.AppID(parsed.appID), parsed.entry)
	w.logger.Debugf(ctx, "Sandbox CRD %q (appID=%s) state updated: actualState=%q host=%q autoRestartEnabled=%v devMode=%v upstreamTarget=%v", parsed.entry.k8sName, parsed.appID, parsed.entry.state, parsed.entry.host, parsed.entry.autoRestartEnabled, parsed.entry.devMode, parsed.entry.upstreamTarget != nil)
}

type parsedObject struct {
	appID string
	entry entry
}

// parseObject converts an App or Sandbox CRD event into a cache entry.
func (w *StateWatcher) parseObject(ctx context.Context, kind string, obj any) (parsedObject, bool) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return parsedObject{}, false
	}
	k8sName := u.GetName()

	var appObj appObject
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &appObj); err != nil {
		w.logger.Errorf(ctx, "failed to convert %s CRD %q: %s", kind, k8sName, err)
		return parsedObject{}, false
	}

	if appObj.Spec.AppID == "" {
		w.logger.Warnf(ctx, "%s CRD %q has empty spec.appId, skipping", kind, k8sName)
		return parsedObject{}, false
	}

	autoRestartEnabled := true
	if appObj.Spec.AutoRestartEnabled != nil {
		autoRestartEnabled = *appObj.Spec.AutoRestartEnabled
	}

	devMode := appObj.Spec.DevMode != nil && appObj.Spec.DevMode.Enabled

	var upstreamTarget *url.URL
	if rawURL := appObj.Status.AppsProxy.UpstreamURL; rawURL != "" {
		if t, err := url.Parse(rawURL); err == nil {
			upstreamTarget = t
		} else {
			w.logger.Warnf(ctx, "%s CRD %q (appID=%s) invalid upstream URL %q from appsProxy.upstreamUrl: %s", kind, k8sName, appObj.Spec.AppID, rawURL, err)
		}
	}

	var host string
	if rawURL := appObj.Status.AppsProxy.PublicURL; rawURL != "" {
		if t, err := url.Parse(rawURL); err == nil {
			host = NormalizeHost(t.Host)
		} else {
			w.logger.Warnf(ctx, "%s CRD %q (appID=%s) invalid public URL %q from appsProxy.publicUrl: %s", kind, k8sName, appObj.Spec.AppID, rawURL, err)
		}
	}

	var autoSuspendAfter *time.Duration
	if secs := appObj.Spec.AutoSuspendAfterSeconds; secs != nil {
		d := time.Duration(*secs) * time.Second
		autoSuspendAfter = &d
	}

	var lastStarted time.Time
	if t := appObj.Status.LastStartedTime; t != nil {
		lastStarted = t.Time
	}

	return parsedObject{
		appID: appObj.Spec.AppID,
		entry: entry{
			k8sName:            k8sName,
			uid:                u.GetUID(),
			productionSandbox:  appObj.Status.ProductionSandbox,
			autoSuspendAfter:   autoSuspendAfter,
			lastStarted:        lastStarted,
			appID:              api.AppID(appObj.Spec.AppID),
			host:               host,
			state:              appObj.Status.CurrentState,
			autoRestartEnabled: autoRestartEnabled,
			devMode:            devMode,
			upstreamTarget:     upstreamTarget,
			workspaceID:        appObj.Status.Workspace.ID,
			notDeployed:        isNotDeployed(appObj.Status.Conditions, u.GetGeneration()),
		},
	}, true
}

// storeSandbox releases the hostname the object claimed before, so a Sandbox
// that republishes does not leave the old one routable.
func (w *StateWatcher) storeSandbox(ctx context.Context, appID api.AppID, e entry) {
	if claimedFrom := w.putSandbox(e); claimedFrom != "" {
		w.logger.Warnf(ctx, "Sandbox CRD %q (appID=%s) claims hostname %q already claimed by Sandbox %q; the newest claim wins", e.k8sName, appID, e.host, claimedFrom)
	}
}

func (w *StateWatcher) putApp(appID api.AppID, e entry) {
	w.routeLock.Lock()
	defer w.routeLock.Unlock()
	w.apps[appID] = e
}

// putSandbox stores the entry and takes its hostname, reporting the Sandbox
// that held the hostname before, if any.
func (w *StateWatcher) putSandbox(e entry) string {
	w.routeLock.Lock()
	defer w.routeLock.Unlock()

	if prev, existed := w.sandboxes[e.k8sName]; existed && prev.host != e.host {
		w.releaseHost(prev.host, e.k8sName)
	}

	var claimedFrom string
	if e.host != "" {
		if owner, ok := w.sandboxHosts[e.host]; ok && owner != e.k8sName {
			claimedFrom = owner
		}
		w.sandboxHosts[e.host] = e.k8sName
	}
	w.sandboxes[e.k8sName] = e
	return claimedFrom
}

// deleteSandbox removes the Sandbox and releases the hostname it held.
func (w *StateWatcher) deleteSandbox(k8sName string) (entry, bool) {
	w.routeLock.Lock()
	defer w.routeLock.Unlock()

	e, found := w.sandboxes[k8sName]
	if found {
		w.releaseHost(e.host, k8sName)
		delete(w.sandboxes, k8sName)
	}
	return e, found
}

// deleteAppByName removes the App cached under a K8s object name and reports
// which app id it held.
func (w *StateWatcher) deleteAppByName(k8sName string) (api.AppID, bool) {
	w.routeLock.Lock()
	defer w.routeLock.Unlock()

	for appID, e := range w.apps {
		if e.k8sName == k8sName {
			delete(w.apps, appID)
			return appID, true
		}
	}
	return "", false
}

// releaseHost removes the hostname from the index. The caller must hold routeLock.
func (w *StateWatcher) releaseHost(host, k8sName string) {
	if host == "" {
		return
	}
	if owner, ok := w.sandboxHosts[host]; ok && owner == k8sName {
		delete(w.sandboxHosts, host)
	}
}

func (w *StateWatcher) handleSandboxDelete(ctx context.Context, obj any) {
	u, ok := objectFromDeleteEvent(obj)
	if !ok {
		return
	}
	k8sName := u.GetName()

	e, found := w.deleteSandbox(k8sName)
	if found {
		w.logger.Debugf(ctx, "Sandbox CRD %q (appID=%s) removed from cache", k8sName, e.appID)
		w.workloadRemovalNotifier.notify(WorkloadRef{AppID: e.appID, SandboxName: k8sName})
	}
}

func objectFromDeleteEvent(obj any) (*unstructured.Unstructured, bool) {
	if u, ok := obj.(*unstructured.Unstructured); ok {
		return u, true
	}
	// Handle tombstone objects from the cache.
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		return nil, false
	}
	u, ok := tombstone.Obj.(*unstructured.Unstructured)
	return u, ok
}

func (w *StateWatcher) handleDelete(ctx context.Context, obj any) {
	u, ok := objectFromDeleteEvent(obj)
	if !ok {
		return
	}
	k8sName := u.GetName()

	removed, found := w.deleteAppByName(k8sName)
	if !found {
		return
	}

	w.logger.Debugf(ctx, "App CRD %q (appID=%s) removed from cache", k8sName, removed)
	w.workloadRemovalNotifier.notify(WorkloadRef{AppID: removed})
}
