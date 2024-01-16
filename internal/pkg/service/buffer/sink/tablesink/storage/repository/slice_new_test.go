package repository

import (
	"testing"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/s3"
	"github.com/relvacode/iso8601"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

func TestNewSlice_InvalidCompressionType(t *testing.T) {
	t.Parallel()

	// Fixtures
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}
	fileKey := storage.FileKey{SinkKey: sinkKey, FileID: storage.FileID{OpenedAt: utctime.From(now)}}
	sink := definition.Sink{
		SinkKey: sinkKey,
		Type:    definition.SinkTypeTable,
		Table: &definition.TableSink{
			Mapping: definition.TableMapping{
				TableID: keboola.MustParseTableID("in.bucket.table"),
				Columns: column.Columns{
					column.Datetime{Name: "datetime"},
					column.Body{Name: "body"},
				},
			},
		},
	}
	credentials := &keboola.FileUploadCredentials{
		S3UploadParams: &s3.UploadParams{
			Credentials: s3.Credentials{
				Expiration: iso8601.Time{Time: now.Add(time.Hour)},
			},
		},
	}
	cfg := storage.NewConfig()

	// Create file
	file, err := newFile(cfg, FileResource{FileKey: fileKey, Credentials: credentials}, sink)
	require.NoError(t, err)

	// Set unsupported compression type
	file.LocalStorage.Compression.Type = compression.TypeZSTD

	// Assert
	_, err = newSlice(now, file, "my-volume", 0)
	require.Error(t, err)
	assert.Equal(t, `file compression type "zstd" is not supported`, err.Error())
}
