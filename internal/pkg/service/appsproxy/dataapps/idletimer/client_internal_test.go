package idletimer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	testNamespace = "sandbox"
	testSelf      = "apps-proxy-test"
)

func newTestClient() *client {
	scheme := runtime.NewScheme()
	dyn := k8sfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		gvr(): "IdleTimerList",
	})
	return newClient(dyn, testNamespace)
}

func TestClient_Get_MissingRecordIsNotFound(t *testing.T) {
	t.Parallel()

	_, err := newTestClient().get(t.Context(), "app-1-dpl-abc")

	require.Error(t, err)
	assert.True(t, k8serrors.IsNotFound(err), "absence must be reported as NotFound, got %v", err)
}

func TestClient_Create_StoresTimestampAndOwner(t *testing.T) {
	t.Parallel()

	c := newTestClient()
	at := time.Date(2026, 10, 8, 14, 18, 5, 123456000, time.UTC)

	require.NoError(t, c.create(t.Context(), "app-1-dpl-abc", "uid-1", at))

	rec, err := c.get(t.Context(), "app-1-dpl-abc")
	require.NoError(t, err)
	assert.True(t, at.Equal(rec.lastRequestAt), "want %s, got %s", at, rec.lastRequestAt)
}

func TestClient_Create_OwnerReferenceNamesTheSandbox(t *testing.T) {
	t.Parallel()

	c := newTestClient()
	require.NoError(t, c.create(t.Context(), "app-1-dpl-abc", "uid-1", time.Now()))

	obj, err := c.dyn.Resource(gvr()).Namespace(testNamespace).Get(t.Context(), "app-1-dpl-abc", metav1.GetOptions{})
	require.NoError(t, err)

	owners := obj.GetOwnerReferences()
	require.Len(t, owners, 1)
	assert.Equal(t, "Sandbox", owners[0].Kind)
	assert.Equal(t, "app-1-dpl-abc", owners[0].Name)
	// The apiserver rejects an ownerReference without a uid, and garbage
	// collection keys on it: a wrong one makes the owner look already deleted.
	assert.Equal(t, k8stypes.UID("uid-1"), owners[0].UID)
}

// Garbage collection is the only thing that deletes these records, so a write
// that drops the ownerReference leaks one per workload forever.
func TestClient_Write_KeepsTheOwnerReference(t *testing.T) {
	t.Parallel()

	c := newTestClient()
	at := time.Date(2026, 10, 8, 14, 0, 0, 0, time.UTC)
	require.NoError(t, c.create(t.Context(), "w", "uid-1", at))

	require.NoError(t, c.write(t.Context(), "w", testSelf, at.Add(time.Minute), entry{aliveAt: at.Add(time.Minute), heartbeat: time.Minute}))

	obj, err := c.dyn.Resource(gvr()).Namespace(testNamespace).Get(t.Context(), "w", metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, obj.GetOwnerReferences(), 1)
	assert.Equal(t, k8stypes.UID("uid-1"), obj.GetOwnerReferences()[0].UID)
}

func TestClient_Write_WritesANewerTimestamp(t *testing.T) {
	t.Parallel()

	c := newTestClient()
	old := time.Date(2026, 10, 8, 14, 0, 0, 0, time.UTC)
	fresh := old.Add(time.Minute)
	require.NoError(t, c.create(t.Context(), "w", "uid-1", old))

	require.NoError(t, c.write(t.Context(), "w", testSelf, fresh, entry{aliveAt: fresh, heartbeat: time.Minute}))

	rec, err := c.get(t.Context(), "w")
	require.NoError(t, err)
	assert.True(t, fresh.Equal(rec.lastRequestAt), "want %s, got %s", fresh, rec.lastRequestAt)
}

func TestClient_Write_StaleWriterCannotMoveItBackwards(t *testing.T) {
	t.Parallel()

	c := newTestClient()
	current := time.Date(2026, 10, 8, 14, 0, 0, 0, time.UTC)
	stale := current.Add(-time.Minute)
	require.NoError(t, c.create(t.Context(), "w", "uid-1", current))

	require.NoError(t, c.write(t.Context(), "w", testSelf, stale, entry{aliveAt: current, heartbeat: time.Minute}))

	rec, err := c.get(t.Context(), "w")
	require.NoError(t, err)
	assert.True(t, current.Equal(rec.lastRequestAt), "want %s untouched, got %s", current, rec.lastRequestAt)
}

// The entry is the replica saying it is still writing, so it is written even
// when the timestamp has nothing new.
func TestClient_Write_AnEqualTimestampStillWritesTheOwnEntry(t *testing.T) {
	t.Parallel()

	c := newTestClient()
	at := time.Date(2026, 10, 8, 14, 0, 0, 0, time.UTC)
	require.NoError(t, c.create(t.Context(), "w", "uid-1", at))

	alive := at.Add(time.Minute)
	require.NoError(t, c.write(t.Context(), "w", testSelf, at, entry{aliveAt: alive, heartbeat: 45 * time.Second}))

	rec, err := c.get(t.Context(), "w")
	require.NoError(t, err)
	assert.True(t, at.Equal(rec.lastRequestAt), "want %s untouched, got %s", at, rec.lastRequestAt)
	assert.Equal(t, entry{aliveAt: alive, heartbeat: 45 * time.Second}, rec.entries[testSelf])
}

func TestClient_Write_KeepsTheOtherReplicasEntry(t *testing.T) {
	t.Parallel()

	c := newTestClient()
	at := time.Date(2026, 10, 8, 14, 0, 0, 0, time.UTC)
	theirs := entry{aliveAt: at, heartbeat: time.Minute}
	require.NoError(t, c.dyn.(*k8sfake.FakeDynamicClient).Tracker().Create(gvr(), timerObject(at, map[string]entry{"other": theirs}), testNamespace))

	require.NoError(t, c.write(t.Context(), "w", testSelf, at, entry{aliveAt: at.Add(time.Second), heartbeat: time.Minute}))

	rec, err := c.get(t.Context(), "w")
	require.NoError(t, err)
	assert.Equal(t, theirs, rec.entries["other"])
	assert.Len(t, rec.entries, 2)
}

// An entry nobody has written for three of its own heartbeats belongs to a pod
// that is gone. Pod names change on every rollout, so leaving it would grow the
// record without end.
func TestClient_Write_DropsAnOverdueEntryOfAnotherReplica(t *testing.T) {
	t.Parallel()

	c := newTestClient()
	at := time.Date(2026, 10, 8, 14, 0, 0, 0, time.UTC)
	gone := entry{aliveAt: at, heartbeat: time.Minute}
	recent := entry{aliveAt: at.Add(2 * time.Minute), heartbeat: time.Minute}
	require.NoError(t, c.dyn.(*k8sfake.FakeDynamicClient).Tracker().Create(gvr(), timerObject(at, map[string]entry{"gone": gone, "recent": recent}), testNamespace))

	now := at.Add(3*time.Minute + time.Second)
	require.NoError(t, c.write(t.Context(), "w", testSelf, at, entry{aliveAt: now, heartbeat: time.Minute}))

	rec, err := c.get(t.Context(), "w")
	require.NoError(t, err)
	assert.NotContains(t, rec.entries, "gone")
	assert.Contains(t, rec.entries, "recent")
	assert.Contains(t, rec.entries, testSelf)
}

func TestClient_Get_ReadsEveryEntry(t *testing.T) {
	t.Parallel()

	c := newTestClient()
	at := time.Date(2026, 10, 8, 14, 0, 0, 123456000, time.UTC)
	want := map[string]entry{
		"a": {aliveAt: at.Add(time.Second), heartbeat: 3 * time.Minute},
		"b": {aliveAt: at.Add(2 * time.Second), heartbeat: 3 * time.Second},
	}
	require.NoError(t, c.dyn.(*k8sfake.FakeDynamicClient).Tracker().Create(gvr(), timerObject(at, want), testNamespace))

	rec, err := c.get(t.Context(), "w")
	require.NoError(t, err)
	assert.Equal(t, want, rec.entries)
}

// A reader that skipped an entry it could not parse would take a smaller
// margin than the writer declared.
func TestClient_Get_AnUnreadableEntryIsAnError(t *testing.T) {
	t.Parallel()

	c := newTestClient()
	obj := timerObject(time.Now(), nil)
	require.NoError(t, unstructured.SetNestedField(obj.Object, map[string]any{
		"a": map[string]any{"aliveAt": formatTime(time.Now()), "heartbeat": "soon"},
	}, "spec", "observers"))
	require.NoError(t, c.dyn.(*k8sfake.FakeDynamicClient).Tracker().Create(gvr(), obj, testNamespace))

	_, err := c.get(t.Context(), "w")
	require.Error(t, err)
}

// On a conflict the value is re-read and the comparison redone, so a writer
// that lost the race does not overwrite the winner with its older timestamp.
// Its own entry is still written.
func TestClient_Write_ConflictRereadsBeforeRetrying(t *testing.T) {
	t.Parallel()

	c := newTestClient()
	start := time.Date(2026, 10, 8, 14, 0, 0, 0, time.UTC)
	mine := start.Add(time.Minute)
	theirs := start.Add(2 * time.Minute)
	require.NoError(t, c.create(t.Context(), "w", "uid-1", start))

	fake := c.dyn.(*k8sfake.FakeDynamicClient)
	var patches []string
	var gets int

	// The reactors stand in for the other replica without writing through the
	// tracker: the fake holds its lock across a reaction, so a nested write
	// from inside one deadlocks.
	fake.PrependReactor("patch", resource, func(a k8stesting.Action) (bool, runtime.Object, error) {
		patches = append(patches, string(a.(k8stesting.PatchAction).GetPatch()))
		if len(patches) == 1 {
			return true, nil, k8serrors.NewConflict(gvr().GroupResource(), "w", errors.New("conflict"))
		}
		return true, nil, nil
	})
	fake.PrependReactor("get", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		gets++
		if gets == 1 {
			return false, nil, nil
		}
		// The re-read after the conflict sees the value that won.
		return true, timerObject(theirs, nil), nil
	})

	require.NoError(t, c.write(t.Context(), "w", testSelf, mine, entry{aliveAt: mine, heartbeat: time.Minute}))

	require.Len(t, patches, 2)
	assert.Equal(t, 2, gets, "the conflict must be followed by a re-read")
	assert.Contains(t, patches[0], formatTime(mine))
	assert.NotContains(t, patches[1], "lastRequestAt", "the losing writer must not retry its own older timestamp")
	assert.Contains(t, patches[1], testSelf)
}

func timerObject(at time.Time, entries map[string]entry) *unstructured.Unstructured {
	spec := map[string]any{"lastRequestAt": formatTime(at)}
	if len(entries) > 0 {
		observers := map[string]any{}
		for self, e := range entries {
			observers[self] = map[string]any{"aliveAt": formatTime(e.aliveAt), "heartbeat": e.heartbeat.String()}
		}
		spec["observers"] = observers
	}
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": k8sapp.Group + "/" + version,
			"kind":       kind,
			"metadata": map[string]any{
				"name":            "w",
				"namespace":       testNamespace,
				"resourceVersion": "999",
			},
			"spec": spec,
		},
	}
}
