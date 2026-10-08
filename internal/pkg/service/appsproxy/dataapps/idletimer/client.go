package idletimer

import (
	"context"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
)

const (
	kind     = "IdleTimer"
	resource = "idletimers"
	version  = "v1"
)

// GVR returns the GroupVersionResource for the IdleTimer CRD.
func GVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: k8sapp.Group, Version: version, Resource: resource}
}

// record is one workload's shared last-seen value, as stored on its IdleTimer.
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
	obj, err := c.dyn.Resource(GVR()).Namespace(c.namespace).Get(ctx, name, metav1.GetOptions{})
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

// create registers the record with an ownerReference to the Sandbox it is named
// after, which is what deletes it: there is no controller for this kind.
func (c *client) create(ctx context.Context, name string, at time.Time) error {
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
					},
				},
			},
			"spec": map[string]any{
				"lastRequestAt": formatTime(at),
			},
		},
	}

	_, err := c.dyn.Resource(GVR()).Namespace(c.namespace).Create(ctx, obj, metav1.CreateOptions{})
	return err
}

// cas raises the stored timestamp to at, and leaves it alone if it is already
// at least that late. Both replicas write the same record, so the maximum is
// maintained here rather than by the reader.
//
// The comparison is redone against the value the conflict exposed, never
// against the one read before it: retrying the original decision is how a
// writer that lost the race drags the timestamp backwards.
func (c *client) cas(ctx context.Context, name string, at time.Time) error {
	for attempt := range 2 {
		rec, err := c.get(ctx, name)
		if err != nil {
			return err
		}
		if !at.After(rec.lastRequestAt) {
			return nil
		}
		if err := c.update(ctx, name, rec.resourceVersion, at); err != nil {
			if attempt == 0 && k8serrors.IsConflict(err) {
				continue
			}
			return err
		}
		return nil
	}
	return nil
}

func (c *client) update(ctx context.Context, name, resourceVersion string, at time.Time) error {
	obj := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": k8sapp.Group + "/" + version,
			"kind":       kind,
			"metadata": map[string]any{
				"name":            name,
				"namespace":       c.namespace,
				"resourceVersion": resourceVersion,
			},
			"spec": map[string]any{
				"lastRequestAt": formatTime(at),
			},
		},
	}

	_, err := c.dyn.Resource(GVR()).Namespace(c.namespace).Update(ctx, obj, metav1.UpdateOptions{})
	return err
}

func formatTime(at time.Time) string {
	return at.UTC().Format(time.RFC3339Nano)
}
