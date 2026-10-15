package idletimer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	kind     = "IdleTimer"
	resource = k8sapp.IdleTimerResource
	version  = k8sapp.IdleTimerVersion
)

func gvr() schema.GroupVersionResource {
	return k8sapp.IdleTimerGVR()
}

type record struct {
	lastRequestAt   time.Time
	entries         map[string]entry
	resourceVersion string
	// cached marks a record read from the watch rather than the apiserver. The
	// watch lags, and a lagging record reads as idle, so a decision taken on
	// one is confirmed before it is acted on.
	cached bool
}

type entry struct {
	aliveAt   time.Time
	heartbeat time.Duration
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

	return parseRecord(obj)
}

// parseRecord reads a record off the object, whether it came from the apiserver
// or from the watch cache.
//
// A record always carries the field: create writes it. An empty one is
// malformed, and must not read as the zero time — a caller that took it would
// measure idleness from 1970 and suspend on the spot.
func parseRecord(obj *unstructured.Unstructured) (record, error) {
	raw, found, err := unstructured.NestedString(obj.Object, "spec", "lastRequestAt")
	if err != nil {
		return record{}, err
	}
	if !found || raw == "" {
		return record{}, errors.Errorf("idle timer %q carries no lastRequestAt", obj.GetName())
	}

	at, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return record{}, err
	}

	entries, err := parseEntries(obj)
	if err != nil {
		return record{}, err
	}

	return record{lastRequestAt: at, entries: entries, resourceVersion: obj.GetResourceVersion()}, nil
}

// parseEntries fails on any entry it cannot read. Skipping one would hand the
// reader a smaller margin than that replica declared.
func parseEntries(obj *unstructured.Unstructured) (map[string]entry, error) {
	raw, found, err := unstructured.NestedMap(obj.Object, "spec", "observers")
	if err != nil || !found {
		return nil, err
	}

	entries := make(map[string]entry, len(raw))
	for self, value := range raw {
		e, err := parseEntry(value)
		if err != nil {
			return nil, errors.PrefixErrorf(err, "idle timer %q carries an unreadable entry %q", obj.GetName(), self)
		}
		entries[self] = e
	}
	return entries, nil
}

func parseEntry(value any) (entry, error) {
	fields, ok := value.(map[string]any)
	if !ok {
		return entry{}, errors.New("not an object")
	}

	aliveAt, err := time.Parse(time.RFC3339Nano, fmt.Sprint(fields["aliveAt"]))
	if err != nil {
		return entry{}, err
	}
	heartbeat, err := time.ParseDuration(fmt.Sprint(fields["heartbeat"]))
	if err != nil {
		return entry{}, err
	}
	return entry{aliveAt: aliveAt, heartbeat: heartbeat}, nil
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

// write raises the stored timestamp to at if that is later, sets this
// replica's own entry, and drops every other entry that is overdue. It writes
// even when the timestamp has nothing new: the entry is how the other replica
// knows this one is still writing.
func (c *client) write(ctx context.Context, name, self string, at time.Time, own entry) error {
	return c.update(ctx, name, func(rec record) map[string]any {
		observers := map[string]any{
			self: map[string]any{"aliveAt": formatTime(own.aliveAt), "heartbeat": own.heartbeat.String()},
		}
		for other, e := range rec.entries {
			if other != self && e.overdue(own.aliveAt) {
				observers[other] = nil
			}
		}
		return raisedTo(rec, at, observers)
	})
}

// leave raises the stored timestamp as write does and takes this replica's
// entry out, in one patch: the entry is what tells a reader the record may
// trail this replica, so it goes only together with the trail.
func (c *client) leave(ctx context.Context, name, self string, at time.Time) error {
	return c.update(ctx, name, func(rec record) map[string]any {
		return raisedTo(rec, at, map[string]any{self: nil})
	})
}

func raisedTo(rec record, at time.Time, observers map[string]any) map[string]any {
	spec := map[string]any{"observers": observers}
	if at.After(rec.lastRequestAt) {
		spec["lastRequestAt"] = formatTime(at)
	}
	return spec
}

// update patches the spec the build function derives from the current record.
// After a conflict the spec is derived again from the value the conflict
// exposed: retrying the original decision is how a writer that lost the race
// drags the timestamp backwards.
//
// It patches only the fields it changes rather than putting the whole object
// back: a full write carries only what this process built, dropping the
// ownerReference and leaking the record. resourceVersion in the body is the
// precondition.
func (c *client) update(ctx context.Context, name string, build func(record) map[string]any) error {
	var lastErr error
	for attempt := range 2 {
		rec, err := c.get(ctx, name)
		if err != nil {
			return err
		}
		lastErr = c.patch(ctx, name, rec.resourceVersion, build(rec))
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

func (c *client) patch(ctx context.Context, name, resourceVersion string, spec map[string]any) error {
	body, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"resourceVersion": resourceVersion},
		"spec":     spec,
	})
	if err != nil {
		return err
	}

	_, err = c.dyn.Resource(gvr()).Namespace(c.namespace).Patch(ctx, name, types.MergePatchType, body, metav1.PatchOptions{})
	return err
}

func formatTime(at time.Time) string {
	return at.UTC().Format(time.RFC3339Nano)
}
