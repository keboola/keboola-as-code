package encoding

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestChunkRetryDuration(t *testing.T) {
	t.Parallel()

	assert.Equal(t, defaultChunkRetryDuration, chunkRetryDuration(0))
	assert.Equal(t, defaultChunkRetryDuration, chunkRetryDuration(-time.Second))
	assert.Equal(t, time.Minute, chunkRetryDuration(time.Minute))
}
