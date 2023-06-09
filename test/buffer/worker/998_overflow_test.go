package worker

import (
	"strings"
	"time"

	"github.com/stretchr/testify/assert"
)

// test998BufferSizeOverflow tests reaching the maximum size of buffered data,
// the API stops accepting requests for the receiver.
func (ts *testSuite) test998BufferSizeOverflow() {
	ts.t.Logf("-------------------------")
	ts.t.Logf("998 buffer size overflow")
	ts.t.Logf("-------------------------")

	// Wait for cache invalidation
	time.Sleep(statisticsSyncInterval + receiverBufferSizeCacheTTL + time.Millisecond)

	// Fill buffer size over the maximum (size is checked before import).
	// Maximum record size (1MB) is not exceeded.
	bytes := strings.Repeat("-", int(receiverBufferSize.Bytes()+1))
	assert.NoError(ts.t, ts.ImportWithPayload(`{"key": "`+bytes+`"}`))

	// Wait for cache invalidation
	time.Sleep(statisticsSyncInterval + receiverBufferSizeCacheTTL + time.Millisecond)

	// Next request is rejected.
	err := ts.ImportWithPayload(`{"key": "foo"}`)
	if assert.Error(ts.t, err) {
		// Note: The request payload is buffered for 2 exports, therefore the size is 200KB+.
		assert.Equal(ts.t, `No free space in the buffer: receiver "my-receiver" has "200.1 KB" buffered for upload, limit is "100.0 KB".`, err.Error())
	}

	ts.TruncateLogs()
}
