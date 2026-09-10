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

// entry stores the K8s object name and last observed state of one workload
// (an App CR or a Sandbox CR).
type entry struct {
	k8sName            string
	appID              api.AppID
	host               string // exact hostname from appsProxy.publicUrl; empty when none is published
	proxyIngress       bool   // spec.features.appsProxyIngress is set, so a hostname will be published
	state              AppActualState
	autoRestartEnabled bool
	devMode            bool
	upstreamTarget     *url.URL // pre-parsed; nil when appsProxy.upstreamUrl absent/invalid
	e2bAccessToken     string   // loaded from K8s Secret; empty for non-E2B apps
	e2bSecretName      string   // Secret name for lazy token loading; empty for non-E2B apps
	tieReported        bool     // this Sandbox's claim on host was already reported as lost to the App
}

// StateWatcher watches App and Sandbox CRDs in Kubernetes and provides a local
// cache of workload states.
type StateWatcher struct {
	client              dynamic.Interface
	namespace           string
	logger              log.Logger
	hasSynced           cache.InformerSynced
	sandboxesHaveSynced cache.InformerSynced
	apps                sync.Map           // api.AppID → entry
	tokenLoadGroup      singleflight.Group // coalesces concurrent lazy-load K8s API calls per secret

	// routeLock guards both hostname indexes. One lock, because a resolve reads
	// them together and must not see the App half and the Sandbox half of a
	// concurrent update.
	routeLock    sync.RWMutex
	sandboxes    map[string]entry     // Sandbox K8s object name → entry
	sandboxHosts map[string]string    // exact hostname → Sandbox K8s object name
	appHosts     map[string]api.AppID // exact hostname → App that published it
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

// NewStateWatcher creates and starts a StateWatcher that watches App and Sandbox CRDs in the given namespace.
// It registers the informer lifecycle with the process.
func NewStateWatcher(d dependencies, client dynamic.Interface, namespace string) *StateWatcher {
	ctx, cancel := context.WithCancelCause(context.Background())
	d.Process().OnShutdown(func(context.Context) {
		cancel(nil)
	})

	appInformer := newCRDInformer(client, AppGVR(), namespace)
	sandboxInformer := newCRDInformer(client, SandboxGVR(), namespace)

	w := &StateWatcher{
		client:              client,
		namespace:           namespace,
		logger:              d.Logger().WithComponent("k8sapp.watcher"),
		hasSynced:           appInformer.HasSynced,
		sandboxesHaveSynced: sandboxInformer.HasSynced,
		sandboxes:           make(map[string]entry),
		sandboxHosts:        make(map[string]string),
		appHosts:            make(map[string]api.AppID),
	}

	w.addEventHandler(ctx, appInformer, "App", w.handleUpsert, w.handleDelete)
	w.addEventHandler(ctx, sandboxInformer, "Sandbox", w.handleSandboxUpsert, w.handleSandboxDelete)

	go appInformer.Run(ctx.Done())
	go sandboxInformer.Run(ctx.Done())

	return w
}

func newCRDInformer(client dynamic.Interface, gvr schema.GroupVersionResource, namespace string) cache.SharedIndexInformer {
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
// If the E2B access token is missing but a secret name is known, it attempts to load the token lazily.
func (w *StateWatcher) GetState(ctx context.Context, ref WorkloadRef) (AppInfo, bool) {
	e, ok := w.entryFor(ref)
	if !ok {
		return AppInfo{}, false
	}

	// Lazy-load E2B token: the Secret may not have existed when the CRD event was processed.
	// a singleflight coalesces concurrent requests for the same secret into a single K8s API call.
	if e.e2bAccessToken == "" && e.e2bSecretName != "" {
		fetchCtx := context.WithoutCancel(ctx)
		token, err, _ := w.tokenLoadGroup.Do(e.e2bSecretName, func() (any, error) {
			return w.loadSecretToken(fetchCtx, e.e2bSecretName)
		})
		if err != nil {
			w.logger.Warnf(ctx, "workload %s: failed to lazy-load E2B access token from secret %q: %s", ref, e.e2bSecretName, err)
		} else if t, ok := token.(string); t != "" && ok {
			e.e2bAccessToken = t
			w.storeEntry(ref, e)
			w.logger.Infof(ctx, "workload %s: lazy-loaded E2B access token from secret %q", ref, e.e2bSecretName)
		}
	}

	return AppInfo{
		ActualState:        e.state,
		AutoRestartEnabled: e.autoRestartEnabled,
		DevMode:            e.devMode,
		UpstreamTarget:     e.upstreamTarget,
		E2BAccessToken:     e.e2bAccessToken,
	}, true
}

// WaitForCacheSync blocks until the informer cache has completed its initial list,
// the stopCh is closed, or the context is cancelled.
// Intended for use in tests to ensure the watch is established before creating objects.
func (w *StateWatcher) WaitForCacheSync(ctx context.Context) bool {
	return cache.WaitForCacheSync(ctx.Done(), w.hasSynced, w.sandboxesHaveSynced)
}

// Wakeup patches spec.state = "Running" on the CRD of the workload that owns
// the route: the Sandbox CR when the route belongs to one, the App CR otherwise.
// If the workload is not in the cache, no patch is sent.
func (w *StateWatcher) Wakeup(ctx context.Context, ref WorkloadRef) error {
	e, ok := w.entryFor(ref)
	if !ok {
		return nil
	}

	gvr := AppGVR()
	if ref.SandboxName != "" {
		gvr = SandboxGVR()
	}

	patch, err := json.Marshal(map[string]any{
		"spec": map[string]any{
			"state": AppActualStateRunning,
		},
	})
	if err != nil {
		return err
	}

	_, err = w.client.Resource(gvr).Namespace(w.namespace).Patch(
		ctx,
		e.k8sName,
		k8stypes.MergePatchType,
		patch,
		metav1.PatchOptions{},
	)
	return err
}

// ResolveWorkload maps a request hostname, and the appID already normalised out
// of it, to the workload that serves the route.
//
// A Sandbox that publishes status.appsProxy.publicUrl owns that exact hostname,
// so the exact-hostname index is consulted first. On any tie the App wins: it
// owns the whole normalised hostname namespace and production traffic must never
// be diverted to a Sandbox.
func (w *StateWatcher) ResolveHost(_ context.Context, host string) (WorkloadRef, bool) {
	// Absence from an unsynced App cache proves nothing, and letting a Sandbox
	// win that race would divert production traffic during startup.
	if !w.hasSynced() {
		return WorkloadRef{}, false
	}

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
	if w.appOwnsHost(e.appID, host) {
		return WorkloadRef{}, false
	}

	return WorkloadRef{AppID: e.appID, SandboxName: k8sName}, true
}

// appOwnsHost reports whether an App keeps the route for this hostname.
// The caller must hold routeLock.
//
// The second clause is the backfill guard: an App whose hostname has not
// reached its status yet is treated as owning the one its own Sandbox claims,
// because during that window its hostname is unknown and today every
// deployment member publishes production's own hostname. It is gated on
// appsProxyIngress being set — without it the App publishes no hostname ever,
// so an ungated clause would be a permanent block rather than a startup one.
func (w *StateWatcher) appOwnsHost(sandboxAppID api.AppID, host string) bool {
	if _, published := w.appHosts[host]; published {
		return true
	}

	appEntry, ok := w.appEntry(sandboxAppID)
	return ok && appEntry.proxyIngress && appEntry.host == ""
}

func (w *StateWatcher) appEntry(appID api.AppID) (entry, bool) {
	v, ok := w.apps.Load(appID)
	if !ok {
		return entry{}, false
	}
	e, ok := v.(entry)
	return e, ok
}

func (w *StateWatcher) entryFor(ref WorkloadRef) (entry, bool) {
	if ref.SandboxName != "" {
		w.routeLock.RLock()
		defer w.routeLock.RUnlock()
		e, ok := w.sandboxes[ref.SandboxName]
		return e, ok
	}

	return w.appEntry(ref.AppID)
}

func (w *StateWatcher) storeEntry(ref WorkloadRef, e entry) {
	if ref.SandboxName == "" {
		w.apps.Store(ref.AppID, e)
		return
	}

	w.routeLock.Lock()
	defer w.routeLock.Unlock()
	if _, ok := w.sandboxes[ref.SandboxName]; ok {
		w.sandboxes[ref.SandboxName] = e
	}
}

func (w *StateWatcher) handleUpsert(ctx context.Context, obj any) {
	parsed, ok := w.parseObject(ctx, "App", obj)
	if !ok {
		return
	}

	appID := api.AppID(parsed.appID)
	prev, existed := w.appEntry(appID)
	w.apps.Store(appID, parsed.entry)

	w.routeLock.Lock()
	if existed && prev.host != parsed.entry.host {
		w.releaseAppHost(prev.host, appID)
	}
	if parsed.entry.host != "" {
		w.appHosts[parsed.entry.host] = appID
	}
	w.routeLock.Unlock()

	if !existed || prev.host != parsed.entry.host {
		w.warnIfClaimedBySandbox(ctx, appID, parsed.entry.host)
	}
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

	// A non-empty status.appsProxy.publicUrl is the operator's statement that
	// this object owns that exact hostname. Nothing is published for a workload
	// that owns no route, so an empty field means "not routable by hostname".
	var host string
	if rawURL := appObj.Status.AppsProxy.PublicURL; rawURL != "" {
		if t, err := url.Parse(rawURL); err == nil {
			host = NormalizeHost(t.Host)
		} else {
			w.logger.Warnf(ctx, "%s CRD %q (appID=%s) invalid public URL %q from appsProxy.publicUrl: %s", kind, k8sName, appObj.Spec.AppID, rawURL, err)
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

	return parsedObject{
		appID: appObj.Spec.AppID,
		entry: entry{
			k8sName:            k8sName,
			appID:              api.AppID(appObj.Spec.AppID),
			host:               host,
			proxyIngress:       appObj.Spec.Features != nil && appObj.Spec.Features.AppsProxyIngress != nil,
			state:              appObj.Status.CurrentState,
			autoRestartEnabled: autoRestartEnabled,
			devMode:            devMode,
			upstreamTarget:     upstreamTarget,
			e2bAccessToken:     e2bAccessToken,
			e2bSecretName:      e2bSecretName,
		},
	}, true
}

// storeSandbox stores the entry and keeps the exact-hostname index in step with
// it, releasing the hostname the object claimed before.
func (w *StateWatcher) storeSandbox(ctx context.Context, appID api.AppID, e entry) {
	var claimedFrom string

	w.routeLock.Lock()
	prev, existed := w.sandboxes[e.k8sName]
	e.tieReported = existed && prev.host == e.host && prev.tieReported
	if existed && prev.host != e.host {
		w.releaseHost(prev.host, e.k8sName)
	}
	if e.host != "" {
		if owner, ok := w.sandboxHosts[e.host]; ok && owner != e.k8sName {
			claimedFrom = owner
		}
		w.sandboxHosts[e.host] = e.k8sName
	}
	reportTie := e.host != "" && !e.tieReported && w.appOwnsHost(appID, e.host)
	if reportTie {
		e.tieReported = true
	}
	w.sandboxes[e.k8sName] = e
	w.routeLock.Unlock()

	if claimedFrom != "" {
		w.logger.Warnf(ctx, "Sandbox CRD %q (appID=%s) claims hostname %q already claimed by Sandbox %q; the newest claim wins", e.k8sName, appID, e.host, claimedFrom)
	}
	if reportTie {
		w.reportTie(ctx, e.k8sName, e.host, appID)
	}
}

// warnIfClaimedBySandbox reports an App hostname that a Sandbox already claims,
// covering the case where the Sandbox was indexed before the App was known.
//
// The tie is reported when a claim is registered, not per request: today every
// deployment member publishes production's own hostname, so the tie is hit on
// every single production request. The tieReported flag is set under
// sandboxLock by both this path and storeSandbox, so concurrent App and Sandbox
// events report the same claim exactly once.
func (w *StateWatcher) warnIfClaimedBySandbox(ctx context.Context, appID api.AppID, host string) {
	if host == "" {
		return
	}

	w.routeLock.Lock()
	k8sName, claimed := w.sandboxHosts[host]
	report := false
	if claimed {
		if e, found := w.sandboxes[k8sName]; found && !e.tieReported {
			e.tieReported = true
			w.sandboxes[k8sName] = e
			report = true
		}
	}
	w.routeLock.Unlock()

	if report {
		w.reportTie(ctx, k8sName, host, appID)
	}
}

func (w *StateWatcher) reportTie(ctx context.Context, k8sName, host string, appID api.AppID) {
	w.logger.Warnf(ctx, "Sandbox CRD %q claims hostname %q owned by App %s; the App keeps the route", k8sName, host, appID)
}

// releaseHost removes the hostname from the index. The caller must hold sandboxLock.
func (w *StateWatcher) releaseHost(host, k8sName string) {
	if host == "" {
		return
	}
	if owner, ok := w.sandboxHosts[host]; ok && owner == k8sName {
		delete(w.sandboxHosts, host)
	}
}

// releaseAppHost removes the App hostname from the index. The caller must hold routeLock.
func (w *StateWatcher) releaseAppHost(host string, appID api.AppID) {
	if host == "" {
		return
	}
	if owner, ok := w.appHosts[host]; ok && owner == appID {
		delete(w.appHosts, host)
	}
}

func (w *StateWatcher) handleSandboxDelete(ctx context.Context, obj any) {
	u, ok := objectFromDeleteEvent(obj)
	if !ok {
		return
	}
	k8sName := u.GetName()

	w.routeLock.Lock()
	e, found := w.sandboxes[k8sName]
	if found {
		w.releaseHost(e.host, k8sName)
	}
	delete(w.sandboxes, k8sName)
	w.routeLock.Unlock()

	w.logger.Debugf(ctx, "Sandbox CRD %q (appID=%s) removed from cache", k8sName, e.appID)
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

	w.apps.Range(func(key, val any) bool {
		e, ok := val.(entry)
		if !ok || e.k8sName != k8sName {
			return true
		}
		w.apps.Delete(key)
		// The App's hostname must be released with it, or a deleted App keeps
		// winning the tie and its Sandboxes stay unroutable forever.
		w.routeLock.Lock()
		w.releaseAppHost(e.host, e.appID)
		w.routeLock.Unlock()
		w.logger.Debugf(ctx, "App CRD %q (appID=%s) removed from cache", k8sName, key)
		return false
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
