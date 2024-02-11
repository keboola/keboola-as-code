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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/level/local/writer/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/level/target"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/volume/assignment"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// fileRepository interface to prevent package import cycle.
type fileRepository interface {
	CloseAllIn(now time.Time, parentKey fmt.Stringer) *op.AtomicOp[op.NoResult]
	Get(fileKey storage.FileKey) op.WithResult[storage.File]
	StateTransition(now time.Time, fileKey storage.FileKey, from, to storage.FileState) *op.AtomicOp[storage.File]
}

func NewFileKey() storage.FileKey {
	return NewFileKeyOpenedAt("2000-01-01T01:00:00.000Z")
}

func NewFileKeyOpenedAt(openedAtStr string) storage.FileKey {
	openedAt := utctime.MustParse(openedAtStr)
	return storage.FileKey{
		SinkKey: NewSinkKey(),
		FileID: storage.FileID{
			OpenedAt: openedAt,
		},
	}
}

func NewFile() storage.File {
	return NewFileOpenedAt("2000-01-01T01:00:00.000Z")
}

func NewFileOpenedAt(openedAtStr string) storage.File {
	openedAt := utctime.MustParse(openedAtStr)
	return storage.File{
		FileKey: NewFileKeyOpenedAt(openedAtStr),
		Type:    storage.FileTypeCSV,
		State:   storage.FileWriting,
		Columns: column.Columns{column.Body{}},
		Assignment: assignment.Assignment{
			Config: assignment.Config{
				Count:          1,
				PreferredTypes: []string{},
			},
			Volumes: []volume.ID{"my-volume"},
		},
		LocalStorage: local.File{
			Dir:         "my-dir",
			Compression: compression.NewNoneConfig(),
			DiskSync:    disksync.NewConfig(),
		},
		StagingStorage: staging.File{
			Compression:                 compression.NewNoneConfig(),
			UploadCredentials:           &keboola.FileUploadCredentials{},
			UploadCredentialsExpiration: utctime.From(openedAt.Time().Add(time.Hour)),
		},
		TargetStorage: target.File{
			TableID:    keboola.MustParseTableID("in.bucket.table"),
			StorageJob: nil,
		},
	}
}

func SwitchFileStates(t *testing.T, ctx context.Context, clk *clock.Mock, fileRepo fileRepository, fileKey storage.FileKey, states []storage.FileState) {
	t.Helper()
	from := states[0]
	for _, to := range states[1:] {
		clk.Add(time.Hour)

		// File must be closed by the CloseAllIn method
		var file storage.File
		var err error
		if to == storage.FileClosing {
			require.Equal(t, storage.FileWriting, from)
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
		case storage.FileClosing:
			assert.Equal(t, utctime.From(clk.Now()).String(), file.ClosingAt.String())
		case storage.FileImporting:
			assert.Equal(t, utctime.From(clk.Now()).String(), file.ImportingAt.String())
		case storage.FileImported:
			assert.Equal(t, utctime.From(clk.Now()).String(), file.ImportedAt.String())
		default:
			panic(errors.Errorf(`unexpected file state "%s"`, to))
		}

		from = to
	}
}

func MockCreateFilesStorageAPICalls(t *testing.T, clk clock.Clock, branchKey key.BranchKey, transport *httpmock.MockTransport) {
	t.Helper()

	fileID := atomic.NewInt32(1000)

	// Mocked file prepare resource endpoint
	transport.RegisterResponder(
		http.MethodPost,
		fmt.Sprintf("/v2/storage/branch/%d/files/prepare", branchKey.BranchID),
		func(request *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(http.StatusOK, &keboola.FileUploadCredentials{
				File: keboola.File{
					FileKey: keboola.FileKey{
						BranchID: branchKey.BranchID,
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
}

func MockDeleteFilesStorageAPICalls(t *testing.T, branchKey key.BranchKey, transport *httpmock.MockTransport) {
	t.Helper()

	// Mocked file delete endpoint
	transport.RegisterResponder(
		http.MethodDelete,
		fmt.Sprintf(`=~/v2/storage/branch/%d/files/\d+$`, branchKey.BranchID),
		httpmock.NewStringResponder(http.StatusNoContent, ""),
	)
}
