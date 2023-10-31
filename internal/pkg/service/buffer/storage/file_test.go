package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local/writer/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/target"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestFileID_Validation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	val := validator.New()

	// Valid
	assert.NoError(t, val.Validate(ctx, FileID{OpenedAt: utctime.MustParse("2006-01-02T15:04:05.000Z")}))

	// Empty
	err := val.Validate(ctx, FileID{})
	if assert.Error(t, err) {
		assert.Equal(t, `"openedAt" is a required field`, err.Error())
	}
}

func TestFileID_String(t *testing.T) {
	t.Parallel()

	// Valid
	assert.Equal(t, "2006-01-02T15:04:05.000Z", (FileID{OpenedAt: utctime.MustParse("2006-01-02T15:04:05.000Z")}).String())

	// Empty
	assert.Panics(t, func() {
		_ = (FileID{}).String()
	})
}

func TestFileKey_Validation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	val := validator.New()

	// Valid
	assert.NoError(t, val.Validate(ctx, testFileKey()))

	// Empty
	err := val.Validate(ctx, FileKey{})
	if assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`
- "projectId" is a required field
- "receiverId" is a required field
- "exportId" is a required field
- "fileId" is a required field
`), strings.TrimSpace(err.Error()))
	}
}

func TestFileKey_String(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "123/my-receiver/my-export/2006-01-02T15:04:05.000Z", testFileKey().String())
}

func TestFileKey_OpenedAt(t *testing.T) {
	t.Parallel()

	openedAt := utctime.MustParse("2006-01-02T15:04:05.000Z")
	k := FileKey{FileID: FileID{OpenedAt: openedAt}}
	assert.Equal(t, openedAt, k.OpenedAt())
}

func TestFile_Validation(t *testing.T) {
	t.Parallel()

	// Following values have own validation
	localStorage := local.File{
		Dir:         "my-dir",
		Compression: compression.DefaultConfig(),
		Sync:        disksync.DefaultConfig(),
		Volumes: local.VolumesAssignment{
			PerPod: 1,
		},
	}
	stagingStorage := staging.File{
		Compression:                 compression.DefaultConfig(),
		UploadCredentials:           &keboola.FileUploadCredentials{},
		UploadCredentialsExpiration: utctime.MustParse("2006-01-02T15:04:05.000Z"),
	}
	targetStorage := target.File{
		TableID:    keboola.MustParseTableID("in.bucket.table"),
		StorageJob: &keboola.StorageJob{},
	}

	// Test cases
	cases := []struct {
		Name          string
		ExpectedError string
		Value         File
	}{
		{
			Name: "empty",
			ExpectedError: `
- "projectId" is a required field
- "receiverId" is a required field
- "exportId" is a required field
- "fileId" is a required field
- "type" is a required field
- "state" is a required field
- "columns" is a required field

`,
			Value: File{
				LocalStorage:   localStorage,
				StagingStorage: stagingStorage,
				TargetStorage:  targetStorage,
			},
		},
		{
			Name:          "empty columns",
			ExpectedError: ` "columns" must contain at least 1 item`,
			Value: File{
				FileKey:        testFileKey(),
				Type:           FileTypeCSV,
				State:          FileWriting,
				Columns:        column.Columns{},
				LocalStorage:   localStorage,
				StagingStorage: stagingStorage,
				TargetStorage:  targetStorage,
			},
		},
		{
			Name: "file state writing",
			Value: File{
				FileKey: testFileKey(),
				Type:    FileTypeCSV,
				State:   FileWriting,
				Columns: column.Columns{
					column.ID{},
					column.Headers{},
					column.Body{},
				},
				LocalStorage:   localStorage,
				StagingStorage: stagingStorage,
				TargetStorage:  targetStorage,
			},
		},
		{
			Name: "file state closing",
			Value: File{
				FileKey:   testFileKey(),
				Type:      FileTypeCSV,
				State:     FileClosing,
				ClosingAt: ptr(utctime.MustParse("2006-01-02T15:04:05.000Z")),
				Columns: column.Columns{
					column.ID{},
					column.Headers{},
					column.Body{},
				},
				LocalStorage:   localStorage,
				StagingStorage: stagingStorage,
				TargetStorage:  targetStorage,
			},
		},
		{
			Name: "file state importing",
			Value: File{
				FileKey:     testFileKey(),
				Type:        FileTypeCSV,
				State:       FileImporting,
				ClosingAt:   ptr(utctime.MustParse("2006-01-02T15:04:05.000Z")),
				ImportingAt: ptr(utctime.MustParse("2006-01-02T16:04:05.000Z")),
				Columns: column.Columns{
					column.ID{},
					column.Headers{},
					column.Body{},
				},
				LocalStorage:   localStorage,
				StagingStorage: stagingStorage,
				TargetStorage:  targetStorage,
			},
		},
		{
			Name: "file state imported",
			Value: File{
				FileKey:     testFileKey(),
				Type:        FileTypeCSV,
				State:       FileImported,
				ClosingAt:   ptr(utctime.MustParse("2006-01-02T15:04:05.000Z")),
				ImportingAt: ptr(utctime.MustParse("2006-01-02T16:04:05.000Z")),
				ImportedAt:  ptr(utctime.MustParse("2006-01-02T17:04:05.000Z")),
				Columns: column.Columns{
					column.ID{},
					column.Headers{},
					column.Body{},
				},
				LocalStorage:   localStorage,
				StagingStorage: stagingStorage,
				TargetStorage:  targetStorage,
			},
		},
	}

	// Run test cases
	ctx := context.Background()
	val := validator.New()
	for _, tc := range cases {
		err := val.Validate(ctx, tc.Value)
		if tc.ExpectedError == "" {
			assert.NoError(t, err, tc.Name)
		} else {
			if assert.Error(t, err, tc.Name) {
				assert.Equal(t, strings.TrimSpace(tc.ExpectedError), strings.TrimSpace(err.Error()), tc.Name)
			}
		}
	}
}

func testFileKey() FileKey {
	return FileKey{
		ExportKey: key.ExportKey{
			ReceiverKey: key.ReceiverKey{
				ProjectID:  123,
				ReceiverID: "my-receiver",
			},
			ExportID: "my-export",
		},
		FileID: FileID{
			OpenedAt: utctime.MustParse("2006-01-02T15:04:05.000Z"),
		},
	}
}
