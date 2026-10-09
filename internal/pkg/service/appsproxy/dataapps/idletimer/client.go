package idletimer

import (
	"context"
	"encoding/json"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
)

const (
	kind     = "IdleTimer"
	resource = "idletimers"
	version  = "v1"
)

// gvr is the IdleTimer CRD this package reads and writes.
func gvr() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: k8sapp.Group, Version: version, Resource: resource}
}

type record struct {
	lastRequestAt   time.Time
	resourceVersion string
}

type client struct {
	dyn       dynamic.Interface
	namespace string
}

func newClient(dyn dynamic.Interface, namespace string) *client {
	return &client{dyn: dyn, namespace: namespace}
}

func (c *client) get(ctx context.Context, name string) (record, error) {
	obj, err := c.dyn.Resource(gvr()).Namespace(c.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return record{}, err
	}

	raw, _, err := unstructured.NestedString(obj.Object, "spec", "lastRequestAt")
	if err != nil {
		return record{}, err
	}

	var at time.Time
	if raw != "" {
		if at, err = time.Parse(time.RFC3339Nano, raw); err != nil {
			return record{}, err
		}
	}

	return record{lastRequestAt: at, resourceVersion: obj.GetResourceVersion()}, nil
}

// create points the record at the Sandbox it is named after, which is the only
// thing that ever deletes it: there is no controller for this kind.
//
// The apiserver rejects an ownerReference with no uid, and collection matches
// on the uid rather than the name, so a stale one reads as an owner that is
// already gone and the record is collected at once.
func (c *client) create(ctx context.Context, name string, ownerUID types.UID, at time.Time) error {
	obj := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": k8sapp.Group + "/" + version,
			"kind":       kind,
			"metadata": map[string]any{
				"name":      name,
				"namespace": c.namespace,
				"ownerReferences": []any{
					map[string]any{
						"apiVersion": k8sapp.Group + "/" + k8sapp.SandboxVersion,
						"kind":       "Sandbox",
						"name":       name,
						"uid":        string(ownerUID),
					},
				},
			},
			"spec": map[string]any{
				"lastRequestAt": formatTime(at),
			},
		},
	}

	_, err := c.dyn.Resource(gvr()).Namespace(c.namespace).Create(ctx, obj, metav1.CreateOptions{})
	return err
}

// cas raises the stored timestamp, leaving it alone if it is already later.
// Both replicas write the same record, so the maximum is maintained here.
//
// After a conflict the comparison is redone against the value the conflict
// exposed: retrying the original decision is how a writer that lost the race
// drags the timestamp backwards.
func (c *client) cas(ctx context.Context, name string, at time.Time) error {
	var lastErr error
	for attempt := range 2 {
		rec, err := c.get(ctx, name)
		if err != nil {
			return err
		}
		if !at.After(rec.lastRequestAt) {
			return nil
		}
		lastErr = c.update(ctx, name, rec.resourceVersion, at)
		if lastErr == nil {
			return nil
		}
		if attempt == 0 && k8serrors.IsConflict(lastErr) {
			continue
		}
		return lastErr
	}
	return lastErr
}

// update patches the one field rather than putting the whole object back: a
// full write carries only what this process built, dropping the ownerReference
// and leaking the record. resourceVersion in the body is the precondition.
func (c *client) update(ctx context.Context, name, resourceVersion string, at time.Time) error {
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"resourceVersion": resourceVersion},
		"spec":     map[string]any{"lastRequestAt": formatTime(at)},
	})
	if err != nil {
		return err
	}

	_, err = c.dyn.Resource(gvr()).Namespace(c.namespace).Patch(
		ctx,
		name,
		types.MergePatchType,
		patch,
		metav1.PatchOptions{},
	)
	return err
}

func formatTime(at time.Time) string {
	return at.UTC().Format(time.RFC3339Nano)
}
