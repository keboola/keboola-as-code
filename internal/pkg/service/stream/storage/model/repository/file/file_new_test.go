package file

import (
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

func TestNewFile_InvalidCompressionType(t *testing.T) {
	t.Parallel()

	// Fixtures
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}
	fileKey := model.FileKey{SinkKey: sinkKey, FileID: model.FileID{OpenedAt: utctime.From(now)}}
	sink := test.NewSinkWithLocalStorage(sinkKey)
	plugins := plugin.New(log.NewNopLogger())
	plugins.RegisterSinkWithLocalStorage(func(sink *definition.Sink) bool {
		return true
	})

	// Set unsupported compression type
	cfg := level.NewConfig()
	cfg.Local.Encoding.Compression.Type = compression.TypeZSTD

	// Assert
	r := &Repository{plugins: plugins}
	_, err := r.newFile(cfg, fileKey, sink)
	require.Error(t, err)
	assert.Equal(t, `file compression type "zstd" is not supported`, err.Error())
}
