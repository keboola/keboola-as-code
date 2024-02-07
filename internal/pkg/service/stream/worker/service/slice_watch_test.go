package service_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/worker/service"
)

func TestActiveSlicesWatcher(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wg := &sync.WaitGroup{}

	// Create watcher
	clk := clock.NewMock()
	clk.Set(time.Time{}.Add(time.Second))
	d, mock := bufferDependencies.NewMockedServiceScope(t, config.NewServiceConfig(), dependencies.WithEnabledEtcdClient(), dependencies.WithClock(clk))
	logger := mock.DebugLogger()

	// Create 2 slices, in writing and closing state
	receiverKey := key.ReceiverKey{ProjectID: 123, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ReceiverKey: receiverKey, ExportID: "my-export"}
	fileKey := key.FileKey{ExportKey: exportKey, FileID: key.FileID(clk.Now())}
	sliceKey1 := key.SliceKey{FileKey: fileKey, SliceID: key.SliceID(clk.Now().Add(1 * time.Second))}
	sliceKey2 := key.SliceKey{FileKey: fileKey, SliceID: key.SliceID(clk.Now().Add(2 * time.Second))}
	mapping := model.MappingForTest(exportKey)
	slice1 := model.Slice{SliceKey: sliceKey1, State: slicestate.Writing, Mapping: mapping, StorageResource: &keboola.FileUploadCredentials{}, Number: 1}
	slice2 := model.Slice{SliceKey: sliceKey2, State: slicestate.Closing, Mapping: mapping, StorageResource: &keboola.FileUploadCredentials{}, Number: 2}
	str := d.Store()
	assert.NoError(t, str.CreateSlice(ctx, slice1))
	assert.NoError(t, str.CreateSlice(ctx, slice2))

	// Create watcher
	w, initDone := service.NewActiveSlicesWatcher(ctx, wg, logger, d.Schema(), d.EtcdClient())
	assert.NoError(t, <-initDone)

	// Wait until all slices are uploaded
	start := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(start)
		assert.NoError(t, w.WaitUntilAllSlicesUploaded(ctx, logger, fileKey))
		logger.Info(ctx, "-----> all slices have been uploaded")
	}()
	<-start

	// Simulate upload of slice2
	assert.NoError(t, str.SetSliceState(ctx, &slice2, slicestate.Uploading))
	assert.NoError(t, str.SetSliceState(ctx, &slice2, slicestate.Failed))
	assert.NoError(t, str.SetSliceState(ctx, &slice2, slicestate.Uploading))
	time.Sleep(100 * time.Millisecond)
	logger.Info(ctx, "-----> slice 2 uploaded")
	assert.NoError(t, str.SetSliceState(ctx, &slice2, slicestate.Uploaded))

	// Simulate upload of slice1
	assert.NoError(t, str.SetSliceState(ctx, &slice1, slicestate.Closing))
	assert.NoError(t, str.SetSliceState(ctx, &slice1, slicestate.Uploading))
	assert.NoError(t, str.SetSliceState(ctx, &slice1, slicestate.Failed))
	assert.NoError(t, str.SetSliceState(ctx, &slice1, slicestate.Uploading))
	time.Sleep(100 * time.Millisecond)
	logger.Info(ctx, "-----> slice 1 uploaded")
	assert.NoError(t, str.SetSliceState(ctx, &slice1, slicestate.Uploaded))

	// Wait
	assert.Eventually(t, func() bool {
		return strings.Contains(logger.AllMessages(), "all slices have been uploaded")
	}, time.Second, 10*time.Millisecond)

	// Check operations order
	logger.AssertJSONMessages(t, `
{"level":"info","message":"waiting for \"2\" slices to be uploaded"}
{"level":"info","message":"-----> slice 2 uploaded"}
{"level":"info","message":"-----> slice 1 uploaded"}
{"level":"info","message":"-----> all slices have been uploaded"}
`)

	// Stop
	cancel()
	wg.Wait()
}
