package slice_test

import (
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	fileRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/file"
	sliceRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/slice"
)

func TestNewSlice_InvalidCompressionType(t *testing.T) {
	t.Parallel()

	// Fixtures
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}
	fileKey := model.FileKey{SinkKey: sinkKey, FileID: model.FileID{OpenedAt: utctime.From(now)}}
	sink := definition.Sink{
		SinkKey: sinkKey,
		Type:    definition.SinkTypeTable,
		Table: &definition.TableSink{
			Type: definition.TableTypeKeboola,
			Keboola: &definition.KeboolaTable{
				TableID: keboola.MustParseTableID("in.bucket.table"),
			},
			Mapping: table.Mapping{
				Columns: column.Columns{
					column.Datetime{Name: "datetime"},
					column.Body{Name: "body"},
				},
			},
		},
	}

	// Create file
	cfg := level.NewConfig()
	file, err := fileRepo.NewFile(cfg, fileKey, sink)
	require.NoError(t, err)

	// Set unsupported compression type
	file.LocalStorage.Compression.Type = compression.TypeZSTD

	// Assert
	_, err = sliceRepo.NewSlice(now, file, "my-volume")
	require.Error(t, err)
	assert.Equal(t, `file compression type "zstd" is not supported`, err.Error())
}
