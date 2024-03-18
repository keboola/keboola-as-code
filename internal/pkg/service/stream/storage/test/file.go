package test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/s3"
	"github.com/relvacode/iso8601"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/assignment"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/disksync"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// fileRepository interface to prevent package import cycle.
type fileRepository interface {
	CloseAllIn(now time.Time, parentKey fmt.Stringer) *op.AtomicOp[op.NoResult]
	Get(fileKey model.FileKey) op.WithResult[model.File]
	StateTransition(now time.Time, fileKey model.FileKey, from, to model.FileState) *op.AtomicOp[model.File]
}

func NewFileKey() model.FileKey {
	return NewFileKeyOpenedAt("2000-01-01T01:00:00.000Z")
}

func NewFileKeyOpenedAt(openedAtStr string) model.FileKey {
	openedAt := utctime.MustParse(openedAtStr)
	return model.FileKey{
		SinkKey: NewSinkKey(),
		FileID: model.FileID{
			OpenedAt: openedAt,
		},
	}
}

func NewFile() model.File {
	return NewFileOpenedAt("2000-01-01T01:00:00.000Z")
}

func NewFileOpenedAt(openedAtStr string) model.File {
	openedAt := utctime.MustParse(openedAtStr)
	fileKey := NewFileKeyOpenedAt(openedAtStr)
	return model.File{
		FileKey: fileKey,
		Type:    model.FileTypeCSV,
		State:   model.FileWriting,
		Columns: column.Columns{column.Body{}},
		Assignment: assignment.Assignment{
			Config: assignment.Config{
				Count:          1,
				PreferredTypes: []string{},
			},
			Volumes: []volume.ID{"my-volume"},
		},
		LocalStorage: local.File{
			Dir:         local.NormalizeDirPath(fileKey.String()),
			Compression: compression.NewNoneConfig(),
			DiskSync:    disksync.NewConfig(),
		},
		StagingStorage: staging.File{
			Compression:                 compression.NewNoneConfig(),
			UploadCredentials:           &keboola.FileUploadCredentials{},
			UploadCredentialsExpiration: utctime.From(openedAt.Time().Add(time.Hour)),
		},
		TargetStorage: target.Target{
			Table: target.Table{
				Keboola: target.KeboolaTable{
					TableID:    keboola.MustParseTableID("in.bucket.table"),
					StorageJob: nil,
				},
			},
		},
	}
}

func SwitchFileStates(t *testing.T, ctx context.Context, clk *clock.Mock, fileRepo fileRepository, fileKey model.FileKey, interval time.Duration, states []model.FileState) {
	t.Helper()
	from := states[0]
	for _, to := range states[1:] {
		clk.Add(interval)

		// File must be closed by the CloseAllIn method
		var file model.File
		var err error
		if to == model.FileClosing {
			require.Equal(t, model.FileWriting, from)
			require.NoError(t, fileRepo.CloseAllIn(clk.Now(), fileKey.SinkKey).Do(ctx).Err())
			file, err = fileRepo.Get(fileKey).Do(ctx).ResultOrErr()
			require.NoError(t, err)
		} else {
			file, err = fileRepo.StateTransition(clk.Now(), fileKey, from, to).Do(ctx).ResultOrErr()
			require.NoError(t, err)
		}

		// File state has been switched
		assert.Equal(t, to, file.State)

		// Retry should be reset
		assert.Equal(t, 0, file.RetryAttempt)
		assert.Nil(t, file.LastFailedAt)

		// Check timestamp
		switch to {
		case model.FileClosing:
			assert.Equal(t, utctime.From(clk.Now()).String(), file.ClosingAt.String())
		case model.FileImporting:
			assert.Equal(t, utctime.From(clk.Now()).String(), file.ImportingAt.String())
		case model.FileImported:
			assert.Equal(t, utctime.From(clk.Now()).String(), file.ImportedAt.String())
		default:
			panic(errors.Errorf(`unexpected file state "%s"`, to))
		}

		from = to
	}
}

func MockFileStorageAPICalls(t *testing.T, clk clock.Clock, transport *httpmock.MockTransport) {
	t.Helper()

	fileID := atomic.NewInt32(1000)

	// Mocked file prepare resource endpoint
	transport.RegisterResponder(
		http.MethodPost,
		`/v2/storage/branch/[0-9]+/files/prepare`,
		func(request *http.Request) (*http.Response, error) {
			branchID, err := extractBranchIDFromURL(request.URL.String())
			if err != nil {
				return nil, err
			}

			return httpmock.NewJsonResponse(http.StatusOK, &keboola.FileUploadCredentials{
				File: keboola.File{
					FileKey: keboola.FileKey{
						BranchID: branchID,
						FileID:   keboola.FileID(fileID.Inc()),
					},
				},
				S3UploadParams: &s3.UploadParams{
					Credentials: s3.Credentials{
						Expiration: iso8601.Time{Time: clk.Now().Add(time.Hour)},
					},
				},
			})
		},
	)

	// Mocked file delete endpoint
	transport.RegisterResponder(
		http.MethodDelete,
		`=~/v2/storage/branch/[0-9]+/files/\d+$`,
		httpmock.NewStringResponder(http.StatusNoContent, ""),
	)
}
