package service_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/service"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

func TestActiveSlicesWatcher(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wg := &sync.WaitGroup{}

	// Create watcher
	clk := clock.NewMock()
	clk.Set(time.Time{}.Add(time.Second))
	d := bufferDependencies.NewMockedDeps(t, dependencies.WithClock(clk))
	logger := d.DebugLogger()

	// Create 2 slices, in writing and closing state
	receiverKey := key.ReceiverKey{ProjectID: 123, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ReceiverKey: receiverKey, ExportID: "my-export"}
	fileKey := key.FileKey{ExportKey: exportKey, FileID: key.FileID(clk.Now())}
	sliceKey1 := key.SliceKey{FileKey: fileKey, SliceID: key.SliceID(clk.Now().Add(1 * time.Second))}
	sliceKey2 := key.SliceKey{FileKey: fileKey, SliceID: key.SliceID(clk.Now().Add(2 * time.Second))}
	mapping := model.MappingForTest(exportKey)
	slice1 := model.Slice{SliceKey: sliceKey1, State: slicestate.Writing, Mapping: mapping, StorageResource: &storageapi.File{}, Number: 1}
	slice2 := model.Slice{SliceKey: sliceKey2, State: slicestate.Closing, Mapping: mapping, StorageResource: &storageapi.File{}, Number: 2}
	str := d.Store()
	assert.NoError(t, str.CreateSlice(ctx, slice1))
	assert.NoError(t, str.CreateSlice(ctx, slice2))

	// Wait until all slices are uploaded
	w, initDone := service.NewActiveSlicesWatcher(ctx, wg, logger, d.Schema(), d.EtcdClient())
	assert.NoError(t, <-initDone)
	start := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(start)
		assert.NoError(t, w.WaitUntilAllSlicesUploaded(ctx, logger, fileKey))
		logger.Info("-----> all slices have been uploaded")
	}()
	<-start

	// Simulate upload of slice2
	assert.NoError(t, str.SetSliceState(ctx, &slice2, slicestate.Uploading))
	assert.NoError(t, str.SetSliceState(ctx, &slice2, slicestate.Failed))
	assert.NoError(t, str.SetSliceState(ctx, &slice2, slicestate.Uploading))
	time.Sleep(100 * time.Millisecond)
	logger.Info("-----> slice 2 uploaded")
	assert.NoError(t, str.SetSliceState(ctx, &slice2, slicestate.Uploaded))

	// Simulate upload of slice1
	assert.NoError(t, str.SetSliceState(ctx, &slice1, slicestate.Closing))
	assert.NoError(t, str.SetSliceState(ctx, &slice1, slicestate.Uploading))
	assert.NoError(t, str.SetSliceState(ctx, &slice1, slicestate.Failed))
	assert.NoError(t, str.SetSliceState(ctx, &slice1, slicestate.Uploading))
	time.Sleep(100 * time.Millisecond)
	logger.Info("-----> slice 1 uploaded")
	assert.NoError(t, str.SetSliceState(ctx, &slice1, slicestate.Uploaded))

	// Wait
	assert.Eventually(t, func() bool {
		return strings.Contains(logger.AllMessages(), "all slices have been uploaded")
	}, time.Second, 10*time.Millisecond)

	// Check operations order
	wildcards.Assert(t, `
INFO  waiting for "2" slices to be uploaded
INFO  -----> slice 2 uploaded
INFO  -----> slice 1 uploaded
INFO  -----> all slices have been uploaded
`, logger.AllMessages())

	// Stop
	cancel()
	wg.Wait()
}
