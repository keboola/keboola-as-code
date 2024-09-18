package telemetry

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileDescriptors(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("file descriptor statistics only work on unix")
	}

	used, err := UsedFileDescriptors()
	require.NoError(t, err)
	assert.NotEqual(t, 0, used)

	total, err := TotalFileDescriptors()
	require.NoError(t, err)
	assert.NotEqual(t, 0, total)

	assert.Less(t, used, int(total))
}
