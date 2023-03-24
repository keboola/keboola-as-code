package worker

import (
	"io"
	"strings"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
)

// test998BufferSizeOverflow tests reaching the maximum size of buffered data,
// the API stops accepting requests for the receiver.
func (ts *testSuite) test998BufferSizeOverflow() {
	// Wait for cache invalidation
	time.Sleep(statisticsSyncInterval + receiverBufferSizeCacheTTL + time.Millisecond)

	// Fill buffer size over the maximum (size is checked before import).
	// Maximum record size (1MB) is not exceeded.
	n := ts.RandomAPINode()
	bytes := strings.Repeat("-", int(receiverBufferSize.Bytes()+1))
	assert.NoError(ts.t, n.Service.Import(n.Dependencies, &buffer.ImportPayload{
		ProjectID:  buffer.ProjectID(ts.project.ID()),
		ReceiverID: ts.receiver.ID,
		Secret:     ts.secret,
	}, io.NopCloser(strings.NewReader(`{"key": "`+bytes+`"}`))))

	// Wait for cache invalidation
	time.Sleep(statisticsSyncInterval + receiverBufferSizeCacheTTL + time.Millisecond)

	// Next request is rejected.
	n = ts.RandomAPINode()
	err := n.Service.Import(n.Dependencies, &buffer.ImportPayload{
		ProjectID:  buffer.ProjectID(ts.project.ID()),
		ReceiverID: ts.receiver.ID,
		Secret:     ts.secret,
	}, io.NopCloser(strings.NewReader(`{"key": "foo"}`)))
	if assert.Error(ts.t, err) {
		// Note: The request payload is buffered for 2 exports, therefore the size is 200KB+.
		assert.Equal(ts.t, `no free space in the buffer: receiver "my-receiver" has "200.1 KB" buffered for upload, limit is "100.0 KB"`, err.Error())
	}

	ts.TruncateLogs()
}
