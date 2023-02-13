package watcher_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/filestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/watcher"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

func TestAPIAndWorkerNodesSync(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := log.NewDebugLogger()
	etcdNamespace := "unit-" + t.Name() + "-" + gonanoid.Must(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)

	d := bufferDependencies.NewMockedDeps(t, dependencies.WithEtcdNamespace(etcdNamespace))
	str := d.Store()

	createDeps := func(nodeName string) bufferDependencies.Mocked {
		nodeDeps := bufferDependencies.NewMockedDeps(
			t,
			dependencies.WithUniqueID(nodeName),
			dependencies.WithLoggerPrefix(fmt.Sprintf("[%s]", nodeName)),
			dependencies.WithEtcdNamespace(etcdNamespace),
		)
		nodeDeps.DebugLogger().ConnectTo(testhelper.VerboseStdout())
		return nodeDeps
	}

	createAPINode := func(nodeName string) *watcher.APINode {
		apiNode, err := watcher.NewAPINode(createDeps(nodeName))
		assert.NoError(t, err)
		return apiNode
	}

	createWorkerNode := func(nodeName string) *watcher.WorkerNode {
		workerNode, err := watcher.NewWorkerNode(createDeps(nodeName))
		assert.NoError(t, err)
		return workerNode
	}

	// Create API and Worker nodes.
	apiNode1 := createAPINode("api-node-1")
	apiNode2 := createAPINode("api-node-2")
	workerNode1 := createWorkerNode("worker-node-1")
	workerNode2 := createWorkerNode("worker-node-2")

	// Create export.
	now := time.Now().UTC()
	sliceKey1, rev1 := createExport(t, ctx, client, str, now)
	receiverKey := sliceKey1.ReceiverKey
	exportKey := sliceKey1.ExportKey

	// There is no revision lock/work in progress,
	// so API nodes immediately report the latest revision to Worker nodes.
	// WaitForRevision calls wait for it and they are immediately unblocked.
	assert.NoError(t, workerNode1.WaitForRevision(ctx, rev1))
	assert.NoError(t, workerNode2.WaitForRevision(ctx, rev1))

	// API nodes do some work based on the current Rev1.
	r1, found, unlock1Rev1 := apiNode1.GetReceiver(receiverKey)
	assert.True(t, found)
	assert.Len(t, r1.Slices, 1)
	assert.Equal(t, sliceKey1.String(), r1.Slices[0].SliceKey.String())
	r2, found, unlock2Rev1 := apiNode2.GetReceiver(receiverKey)
	assert.True(t, found)
	assert.Len(t, r2.Slices, 1)
	assert.Equal(t, sliceKey1.String(), r2.Slices[0].SliceKey.String())

	// Update export, create new slice, close old slice.
	sliceKey2, rev2 := updateExport(t, ctx, client, str, exportKey, now)

	// Wait until the change is propagated to API nodes.
	assert.Eventually(t, func() bool {
		return apiNode1.StateRev() == rev2 && apiNode2.StateRev() == rev2
	}, time.Second, 10*time.Millisecond, "timeout")

	// Rev1 is still locked
	assert.Equal(t, rev1, apiNode1.MinRevInUse())
	assert.Equal(t, rev1, apiNode2.MinRevInUse())

	// API nodes do some work based on the current Rev2.
	r1, found, unlock1Rev2 := apiNode1.GetReceiver(receiverKey)
	assert.True(t, found)
	assert.Len(t, r1.Slices, 1)
	assert.Equal(t, sliceKey2.String(), r1.Slices[0].SliceKey.String())
	r2, found, unlock2Rev2 := apiNode2.GetReceiver(receiverKey)
	assert.True(t, found)
	assert.Len(t, r2.Slices, 1)
	assert.Equal(t, sliceKey2.String(), r2.Slices[0].SliceKey.String())

	// The new revision Rev2 will be reported by API nodes ONLY AFTER
	// all the work with the older Rev1 is completed (unlock1Rev1, unlock2Rev1).
	done1, done2, done3, done4 := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done1)
		assert.NoError(t, workerNode1.WaitForRevision(ctx, rev2))
		logger.Info("unblocked")
	}()
	go func() {
		defer close(done2)
		assert.NoError(t, workerNode2.WaitForRevision(ctx, rev2))
		logger.Info("unblocked")
	}()

	// Goroutines above are blocked until work on the previous revision Rev1 is completed.
	// Simulate work with some delay.
	go func() {
		defer close(done3)
		time.Sleep(100 * time.Millisecond)
		logger.Info("work1 in API node done")
		unlock1Rev1()
	}()
	go func() {
		defer close(done4)
		time.Sleep(200 * time.Millisecond)
		logger.Info("work2 in API node done")
		unlock2Rev1()
	}()
	// Wait
	for i, ch := range []chan struct{}{done1, done2, done3, done4} {
		select {
		case <-ch:
			// ok
		case <-time.After(5 * time.Second):
			t.Fatal("timeout", i+1)
		}
	}

	// Check order of the operations
	expected := `
INFO  work1 in API node done
INFO  work2 in API node done
INFO  unblocked
INFO  unblocked
`
	assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(logger.AllMessages()))

	// Work with Rev2 revision is also done
	unlock1Rev2()
	unlock2Rev2()
}

// createReceiver creates receiver,export,mapping,file and slice.
func createExport(t *testing.T, ctx context.Context, client *etcd.Client, str *store.Store, now time.Time) (key.SliceKey, int64) {
	t.Helper()
	receiver := model.ReceiverForTest("my-receiver", 1, now)
	header := etcdhelper.ExpectModification(t, client, func() {
		assert.NoError(t, str.CreateReceiver(ctx, receiver))
	})
	return receiver.Exports[0].OpenedSlice.SliceKey, header.Revision
}

// updateExport updates export and mapping, creates new file and slice.
func updateExport(t *testing.T, ctx context.Context, client *etcd.Client, str *store.Store, exportKey key.ExportKey, now time.Time) (key.SliceKey, int64) {
	t.Helper()

	fileKey2 := key.FileKey{ExportKey: exportKey, FileID: key.FileID(now.Add(time.Hour))}
	sliceKey2 := key.SliceKey{FileKey: fileKey2, SliceID: key.SliceID(now.Add(time.Hour))}

	header := etcdhelper.ExpectModification(t, client, func() {
		assert.NoError(t, str.UpdateExport(ctx, exportKey, func(export model.Export) (model.Export, error) {
			newMapping := export.Mapping
			newMapping.Columns = []column.Column{column.ID{Name: "id"}, column.Body{Name: "body"}, column.IP{Name: "ip"}}
			export.Mapping = newMapping
			export.OpenedFile = model.File{
				FileKey:         fileKey2,
				State:           filestate.Opened,
				Mapping:         newMapping,
				StorageResource: &keboola.FileUploadCredentials{},
			}
			export.OpenedSlice = model.Slice{
				SliceKey:        sliceKey2,
				State:           slicestate.Writing,
				Mapping:         newMapping,
				StorageResource: &keboola.FileUploadCredentials{},
				Number:          1,
			}
			return export, nil
		}))
	})

	return sliceKey2, header.Revision
}
