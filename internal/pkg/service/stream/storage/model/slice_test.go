package model

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	encoding "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/config"
	localModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/model"
	stagingModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/model"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestVolumeKey_Validation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	val := validator.New()

	// Valid
	assert.NoError(t, val.Validate(ctx, testFileVolumeKey()))

	// Empty
	err := val.Validate(ctx, FileVolumeKey{})
	if assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`
- "projectId" is a required field
- "branchId" is a required field
- "sourceId" is a required field
- "sinkId" is a required field
- "fileOpenedAt" is a required field
- "volumeId" is a required field
`), strings.TrimSpace(err.Error()))
	}
}

func TestFileVolumeKey_String(t *testing.T) {
	t.Parallel()

	// Valid
	assert.Equal(
		t,
		"123/456/my-source/my-sink/2006-01-02T15:04:05.000Z/abcdef",
		testFileVolumeKey().String(),
	)

	// Empty ID
	assert.Panics(t, func() {
		_ = (FileVolumeKey{FileKey: testFileKey(), VolumeID: ""}).String()
	})
}

func TestSliceID_Validation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	val := validator.New()

	// Valid
	assert.NoError(t, val.Validate(ctx, SliceID{
		OpenedAt: utctime.MustParse("2006-01-02T15:04:05.000Z"),
	}))

	// Empty
	err := val.Validate(ctx, SliceID{})
	if assert.Error(t, err) {
		assert.Equal(t, `"sliceOpenedAt" is a required field`, err.Error())
	}
}

func TestSliceID_String(t *testing.T) {
	t.Parallel()

	// Valid
	assert.Equal(t, "2006-01-02T15:04:05.000Z", (SliceID{
		OpenedAt: utctime.MustParse("2006-01-02T15:04:05.000Z"),
	}).String())

	// Empty OpenedAt
	assert.Panics(t, func() {
		_ = (SliceID{}).String()
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
- "branchId" is a required field
- "sourceId" is a required field
- "sinkId" is a required field
- "fileOpenedAt" is a required field
- "volumeId" is a required field
- "sliceOpenedAt" is a required field
`), strings.TrimSpace(err.Error()))
	}
}

func TestSliceKey_String(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "123/456/my-source/my-sink/2006-01-02T10:04:05.000Z/abcdef/2006-01-02T20:04:05.000Z", testSliceKey().String())
}

func TestSliceKey_OpenedAt(t *testing.T) {
	t.Parallel()

	fileOpenedAt := utctime.MustParse("2006-01-02T15:04:05.000Z")
	sliceOpenedAt := utctime.MustParse("2006-01-02T16:04:05.000Z")
	k := SliceKey{
		FileVolumeKey: FileVolumeKey{
			FileKey: FileKey{
				FileID: FileID{
					OpenedAt: fileOpenedAt,
				},
			},
			VolumeID: "abcdef",
		},
		SliceID: SliceID{
			OpenedAt: sliceOpenedAt,
		},
	}

	assert.Equal(t, sliceOpenedAt, k.OpenedAt())
}

func TestSlice_Validation(t *testing.T) {
	t.Parallel()

	// Following values have own validation
	localStorage := localModel.Slice{
		Dir:               "my-dir",
		FilenamePrefix:    "slice",
		FilenameExtension: "csv",
	}
	stagingStorage := stagingModel.Slice{
		Path:        "slice.csv.gzip",
		Compression: compression.NewConfig(),
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
- "branchId" is a required field
- "sourceId" is a required field
- "sinkId" is a required field
- "fileOpenedAt" is a required field
- "volumeId" is a required field
- "sliceOpenedAt" is a required field
- "state" is a required field
- "mapping.columns" is a required field
- "encoding.encoder.type" is a required field
- "encoding.compression.type" is a required field
- "encoding.sync.mode" is a required field
- "local.dir" is a required field
- "local.filenamePrefix" is a required field
- "local.filenameExtension" is a required field
- "staging.path" is a required field
- "staging.compression.type" is a required field
`,
			Value: Slice{},
		},
		{
			Name:          "empty columns",
			ExpectedError: ` "mapping.columns" must contain at least 1 item`,
			Value: Slice{
				SliceKey: testSliceKey(),
				State:    SliceWriting,
				Mapping: table.Mapping{
					Columns: column.Columns{},
				},
				Encoding:       encoding.NewConfig(),
				LocalStorage:   localStorage,
				StagingStorage: stagingStorage,
			},
		},
		{
			Name: "slice state writing",
			Value: Slice{
				SliceKey: testSliceKey(),
				State:    SliceWriting,
				Mapping: table.Mapping{
					Columns: column.Columns{
						column.UUID{Name: "uuid"},
						column.Headers{Name: "headers"},
						column.Body{Name: "body"},
					},
				},
				Encoding:       encoding.NewConfig(),
				LocalStorage:   localStorage,
				StagingStorage: stagingStorage,
			},
		},
		{
			Name: "slice state closing",
			Value: Slice{
				SliceKey:  testSliceKey(),
				State:     SliceClosing,
				ClosingAt: ptr.Ptr(utctime.MustParse("2006-01-02T15:04:05.000Z")),
				Mapping: table.Mapping{
					Columns: column.Columns{
						column.UUID{Name: "uuid"},
						column.Headers{Name: "headers"},
						column.Body{Name: "body"},
					},
				},
				Encoding:       encoding.NewConfig(),
				LocalStorage:   localStorage,
				StagingStorage: stagingStorage,
			},
		},
		{
			Name: "slice state uploading",
			Value: Slice{
				SliceKey:    testSliceKey(),
				State:       SliceUploading,
				ClosingAt:   ptr.Ptr(utctime.MustParse("2006-01-02T15:04:05.000Z")),
				UploadingAt: ptr.Ptr(utctime.MustParse("2006-01-02T16:04:05.000Z")),
				Mapping: table.Mapping{
					Columns: column.Columns{
						column.UUID{Name: "uuid"},
						column.Headers{Name: "headers"},
						column.Body{Name: "body"},
					},
				},
				Encoding:       encoding.NewConfig(),
				LocalStorage:   localStorage,
				StagingStorage: stagingStorage,
			},
		},
		{
			Name: "slice state uploaded",
			Value: Slice{
				SliceKey:    testSliceKey(),
				State:       SliceUploaded,
				ClosingAt:   ptr.Ptr(utctime.MustParse("2006-01-02T15:04:05.000Z")),
				UploadingAt: ptr.Ptr(utctime.MustParse("2006-01-02T16:04:05.000Z")),
				UploadedAt:  ptr.Ptr(utctime.MustParse("2006-01-02T17:04:05.000Z")),
				Mapping: table.Mapping{
					Columns: column.Columns{
						column.UUID{Name: "uuid"},
						column.Headers{Name: "headers"},
						column.Body{Name: "body"},
					},
				},
				Encoding:       encoding.NewConfig(),
				LocalStorage:   localStorage,
				StagingStorage: stagingStorage,
			},
		},
		{
			Name: "slice state imported",
			Value: Slice{
				SliceKey:    testSliceKey(),
				State:       SliceImported,
				ClosingAt:   ptr.Ptr(utctime.MustParse("2006-01-02T15:04:05.000Z")),
				UploadingAt: ptr.Ptr(utctime.MustParse("2006-01-02T16:04:05.000Z")),
				UploadedAt:  ptr.Ptr(utctime.MustParse("2006-01-02T17:04:05.000Z")),
				ImportedAt:  ptr.Ptr(utctime.MustParse("2006-01-02T18:04:05.000Z")),
				Mapping: table.Mapping{
					Columns: column.Columns{
						column.UUID{Name: "uuid"},
						column.Headers{Name: "headers"},
						column.Body{Name: "body"},
					},
				},
				Encoding:       encoding.NewConfig(),
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
		} else if assert.Error(t, err, tc.Name) {
			assert.Equal(t, strings.TrimSpace(tc.ExpectedError), strings.TrimSpace(err.Error()), tc.Name)
		}
	}
}

func testFileVolumeKey() FileVolumeKey {
	return FileVolumeKey{FileKey: testFileKey(), VolumeID: "abcdef"}
}

func testSliceKey() SliceKey {
	return SliceKey{
		FileVolumeKey: FileVolumeKey{
			FileKey: FileKey{
				SinkKey: key.SinkKey{
					SourceKey: key.SourceKey{
						BranchKey: key.BranchKey{
							ProjectID: 123,
							BranchID:  456,
						},
						SourceID: "my-source",
					},
					SinkID: "my-sink",
				},
				FileID: FileID{
					OpenedAt: utctime.MustParse("2006-01-02T10:04:05.000Z"),
				},
			},
			VolumeID: "abcdef",
		},
		SliceID: SliceID{
			OpenedAt: utctime.MustParse("2006-01-02T20:04:05.000Z"),
		},
	}
}
