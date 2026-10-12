package idletimer

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
)

// recordCache serves idle timer reads from a watch. A round reads one record
// per running workload, and the client they share is rate limited, so reading
// them from the apiserver caps how many workloads a replica can cover.
type recordCache struct {
	namespace string
	store     cache.Store
	hasSynced func() bool
}

func newRecordCache(ctx context.Context, dyn dynamic.Interface, namespace string) *recordCache {
	informer := k8sapp.NewCRDInformer(dyn, gvr(), namespace)
	go informer.Run(ctx.Done())

	return &recordCache{
		namespace: namespace,
		store:     informer.GetStore(),
		hasSynced: informer.HasSynced,
	}
}

// lookup reports the cached record. Anything unexpected reads as a miss, which
// costs a live read rather than a wrong answer.
func (rc *recordCache) lookup(name string) (record, bool) {
	if rc == nil || !rc.hasSynced() {
		return record{}, false
	}

	item, found, err := rc.store.GetByKey(rc.namespace + "/" + name)
	if err != nil || !found {
		return record{}, false
	}

	obj, ok := item.(*unstructured.Unstructured)
	if !ok {
		return record{}, false
	}

	raw, found, err := unstructured.NestedString(obj.Object, "spec", "lastRequestAt")
	if err != nil || !found || raw == "" {
		return record{}, false
	}

	at, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return record{}, false
	}

	return record{lastRequestAt: at, resourceVersion: obj.GetResourceVersion(), cached: true}, true
}
