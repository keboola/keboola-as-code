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

const testNamespace = "sandbox"

func newTestClient() *client {
	scheme := runtime.NewScheme()
	dyn := k8sfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		GVR(): "IdleTimerList",
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

	obj, err := c.dyn.Resource(GVR()).Namespace(testNamespace).Get(t.Context(), "app-1-dpl-abc", metav1.GetOptions{})
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
func TestClient_CAS_KeepsTheOwnerReference(t *testing.T) {
	t.Parallel()

	c := newTestClient()
	at := time.Date(2026, 10, 8, 14, 0, 0, 0, time.UTC)
	require.NoError(t, c.create(t.Context(), "w", "uid-1", at))

	require.NoError(t, c.cas(t.Context(), "w", at.Add(time.Minute)))

	obj, err := c.dyn.Resource(GVR()).Namespace(testNamespace).Get(t.Context(), "w", metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, obj.GetOwnerReferences(), 1)
	assert.Equal(t, k8stypes.UID("uid-1"), obj.GetOwnerReferences()[0].UID)
}

func TestClient_CAS_WritesANewerTimestamp(t *testing.T) {
	t.Parallel()

	c := newTestClient()
	old := time.Date(2026, 10, 8, 14, 0, 0, 0, time.UTC)
	fresh := old.Add(time.Minute)
	require.NoError(t, c.create(t.Context(), "w", "uid-1", old))

	require.NoError(t, c.cas(t.Context(), "w", fresh))

	rec, err := c.get(t.Context(), "w")
	require.NoError(t, err)
	assert.True(t, fresh.Equal(rec.lastRequestAt), "want %s, got %s", fresh, rec.lastRequestAt)
}

func TestClient_CAS_StaleWriterCannotMoveItBackwards(t *testing.T) {
	t.Parallel()

	c := newTestClient()
	current := time.Date(2026, 10, 8, 14, 0, 0, 0, time.UTC)
	stale := current.Add(-time.Minute)
	require.NoError(t, c.create(t.Context(), "w", "uid-1", current))

	require.NoError(t, c.cas(t.Context(), "w", stale))

	rec, err := c.get(t.Context(), "w")
	require.NoError(t, err)
	assert.True(t, current.Equal(rec.lastRequestAt), "want %s untouched, got %s", current, rec.lastRequestAt)
}

func TestClient_CAS_EqualTimestampIsNotWritten(t *testing.T) {
	t.Parallel()

	c := newTestClient()
	at := time.Date(2026, 10, 8, 14, 0, 0, 0, time.UTC)
	require.NoError(t, c.create(t.Context(), "w", "uid-1", at))

	before, err := c.get(t.Context(), "w")
	require.NoError(t, err)

	require.NoError(t, c.cas(t.Context(), "w", at))

	after, err := c.get(t.Context(), "w")
	require.NoError(t, err)
	assert.Equal(t, before.resourceVersion, after.resourceVersion, "an equal timestamp must not cost a write")
}

// On a conflict the value is re-read and the comparison redone, so a writer
// that lost the race does not overwrite the winner with its older timestamp.
func TestClient_CAS_ConflictRereadsBeforeRetrying(t *testing.T) {
	t.Parallel()

	c := newTestClient()
	start := time.Date(2026, 10, 8, 14, 0, 0, 0, time.UTC)
	mine := start.Add(time.Minute)
	theirs := start.Add(2 * time.Minute)
	require.NoError(t, c.create(t.Context(), "w", "uid-1", start))

	fake := c.dyn.(*k8sfake.FakeDynamicClient)
	var updates, gets int

	// The reactors stand in for the other replica without writing through the
	// tracker: the fake holds its lock across a reaction, so a nested write
	// from inside one deadlocks.
	fake.PrependReactor("patch", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		updates++
		if updates == 1 {
			return true, nil, k8serrors.NewConflict(GVR().GroupResource(), "w", errors.New("conflict"))
		}
		return true, nil, nil
	})
	fake.PrependReactor("get", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		gets++
		if gets == 1 {
			return false, nil, nil
		}
		// The re-read after the conflict sees the value that won.
		return true, timerObject("w", theirs), nil
	})

	require.NoError(t, c.cas(t.Context(), "w", mine))

	assert.Equal(t, 1, updates, "the losing writer must not retry its own older timestamp")
	assert.Equal(t, 2, gets, "the conflict must be followed by a re-read")
}

func timerObject(name string, at time.Time) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": k8sapp.Group + "/" + version,
			"kind":       kind,
			"metadata": map[string]any{
				"name":            name,
				"namespace":       testNamespace,
				"resourceVersion": "999",
			},
			"spec": map[string]any{"lastRequestAt": formatTime(at)},
		},
	}
}
