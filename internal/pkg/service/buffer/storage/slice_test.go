package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local/writer/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestSliceID_Validation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	val := validator.New()

	// Valid
	assert.NoError(t, val.Validate(ctx, SliceID{
		VolumeID: "abcdef",
		OpenedAt: utctime.MustParse("2006-01-02T15:04:05.000Z"),
	}))

	// Empty
	err := val.Validate(ctx, SliceID{})
	if assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`
- "volumeID" is a required field
- "openedAt" is a required field
`), strings.TrimSpace(err.Error()))
	}
}

func TestSliceID_String(t *testing.T) {
	t.Parallel()

	// Valid
	assert.Equal(t, "abcdef/2006-01-02T15:04:05.000Z", (SliceID{
		VolumeID: "abcdef",
		OpenedAt: utctime.MustParse("2006-01-02T15:04:05.000Z"),
	}).String())

	// Empty VolumeID
	assert.Panics(t, func() {
		_ = (SliceID{OpenedAt: utctime.MustParse("2006-01-02T15:04:05.000Z")}).String()
	})

	// Empty OpenedAt
	assert.Panics(t, func() {
		_ = (SliceID{VolumeID: "abcdef"}).String()
	})
}

func TestSliceKey_Validation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	val := validator.New()

	// Valid
	assert.NoError(t, val.Validate(ctx, testSliceKey()))

	// Empty
	err := val.Validate(ctx, SliceKey{})
	if assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`
- "projectId" is a required field
- "receiverId" is a required field
- "exportId" is a required field
- "fileId" is a required field
- "volumeID" is a required field
- "openedAt" is a required field
`), strings.TrimSpace(err.Error()))
	}
}

func TestSliceKey_String(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "123/my-receiver/my-export/2006-01-02T10:04:05.000Z/abcdef/2006-01-02T20:04:05.000Z", testSliceKey().String())
}

func TestSliceKey_OpenedAt(t *testing.T) {
	t.Parallel()

	fileOpenedAt := utctime.MustParse("2006-01-02T15:04:05.000Z")
	sliceOpenedAt := utctime.MustParse("2006-01-02T16:04:05.000Z")
	k := SliceKey{
		FileKey: FileKey{
			FileID: FileID{
				OpenedAt: fileOpenedAt,
			},
		},
		SliceID: SliceID{
			VolumeID: "abcdef",
			OpenedAt: sliceOpenedAt,
		},
	}

	assert.Equal(t, sliceOpenedAt, k.OpenedAt())
}

func TestSlice_Validation(t *testing.T) {
	t.Parallel()

	// Following values have own validation
	localStorage := local.Slice{
		Dir:         "my-dir",
		Filename:    "slice.csv",
		Compression: compression.DefaultConfig(),
		Sync:        disksync.DefaultConfig(),
	}
	stagingStorage := staging.Slice{
		Path:        "slice.csv.gzip",
		Compression: compression.DefaultConfig(),
	}

	// Test cases
	cases := []struct {
		Name          string
		ExpectedError string
		Value         Slice
	}{
		{
			Name: "empty",
			ExpectedError: `
- "projectId" is a required field
- "receiverId" is a required field
- "exportId" is a required field
- "fileId" is a required field
- "volumeID" is a required field
- "openedAt" is a required field
- "type" is a required field
- "state" is a required field
- "columns" is a required field
`,
			Value: Slice{
				LocalStorage:   localStorage,
				StagingStorage: stagingStorage,
			},
		},
		{
			Name:          "empty columns",
			ExpectedError: ` "columns" must contain at least 1 item`,
			Value: Slice{
				SliceKey:       testSliceKey(),
				Type:           FileTypeCSV,
				State:          SliceWriting,
				Columns:        column.Columns{},
				LocalStorage:   localStorage,
				StagingStorage: stagingStorage,
			},
		},
		{
			Name: "slice state writing",
			Value: Slice{
				SliceKey: testSliceKey(),
				Type:     FileTypeCSV,
				State:    SliceWriting,
				Columns: column.Columns{
					column.ID{},
					column.Headers{},
					column.Body{},
				},
				LocalStorage:   localStorage,
				StagingStorage: stagingStorage,
			},
		},
		{
			Name: "slice state closing",
			Value: Slice{
				SliceKey:  testSliceKey(),
				Type:      FileTypeCSV,
				State:     SliceClosing,
				ClosingAt: ptr(utctime.MustParse("2006-01-02T15:04:05.000Z")),
				Columns: column.Columns{
					column.ID{},
					column.Headers{},
					column.Body{},
				},
				LocalStorage:   localStorage,
				StagingStorage: stagingStorage,
			},
		},
		{
			Name: "slice state uploading",
			Value: Slice{
				SliceKey:    testSliceKey(),
				Type:        FileTypeCSV,
				State:       SliceUploading,
				ClosingAt:   ptr(utctime.MustParse("2006-01-02T15:04:05.000Z")),
				UploadingAt: ptr(utctime.MustParse("2006-01-02T16:04:05.000Z")),
				Columns: column.Columns{
					column.ID{},
					column.Headers{},
					column.Body{},
				},
				LocalStorage:   localStorage,
				StagingStorage: stagingStorage,
			},
		},
		{
			Name: "slice state uploaded",
			Value: Slice{
				SliceKey:    testSliceKey(),
				Type:        FileTypeCSV,
				State:       SliceUploaded,
				ClosingAt:   ptr(utctime.MustParse("2006-01-02T15:04:05.000Z")),
				UploadingAt: ptr(utctime.MustParse("2006-01-02T16:04:05.000Z")),
				UploadedAt:  ptr(utctime.MustParse("2006-01-02T17:04:05.000Z")),
				Columns: column.Columns{
					column.ID{},
					column.Headers{},
					column.Body{},
				},
				LocalStorage:   localStorage,
				StagingStorage: stagingStorage,
			},
		},
		{
			Name: "slice state imported",
			Value: Slice{
				SliceKey:    testSliceKey(),
				Type:        FileTypeCSV,
				State:       SliceImported,
				ClosingAt:   ptr(utctime.MustParse("2006-01-02T15:04:05.000Z")),
				UploadingAt: ptr(utctime.MustParse("2006-01-02T16:04:05.000Z")),
				UploadedAt:  ptr(utctime.MustParse("2006-01-02T17:04:05.000Z")),
				ImportedAt:  ptr(utctime.MustParse("2006-01-02T18:04:05.000Z")),
				Columns: column.Columns{
					column.ID{},
					column.Headers{},
					column.Body{},
				},
				LocalStorage:   localStorage,
				StagingStorage: stagingStorage,
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

func testSliceKey() SliceKey {
	return SliceKey{
		FileKey: FileKey{
			ExportKey: key.ExportKey{
				ReceiverKey: key.ReceiverKey{
					ProjectID:  123,
					ReceiverID: "my-receiver",
				},
				ExportID: "my-export",
			},
			FileID: FileID{
				OpenedAt: utctime.MustParse("2006-01-02T10:04:05.000Z"),
			},
		},
		SliceID: SliceID{
			VolumeID: "abcdef",
			OpenedAt: utctime.MustParse("2006-01-02T20:04:05.000Z"),
		},
	}
}
