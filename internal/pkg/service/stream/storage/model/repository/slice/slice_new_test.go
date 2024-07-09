package slice

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

func TestNewSlice_InvalidCompressionType(t *testing.T) {
	t.Parallel()

	// Fixtures
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()

	// Create file
	file := model.File{}

	// Set unsupported compression type
	file.LocalStorage.Compression.Type = compression.TypeZSTD

	// Assert
	r := &Repository{}
	_, err := r.newSlice(now, file, "my-volume")
	require.Error(t, err)
	assert.Equal(t, `file compression type "zstd" is not supported`, err.Error())
}
