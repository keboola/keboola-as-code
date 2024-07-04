package model

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	localModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/source/writesync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/assignment"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/writernode/diskalloc"
	stagingModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/model"
	targetModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/model"
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
		assert.Equal(t, `"fileOpenedAt" is a required field`, err.Error())
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
- "branchId" is a required field
- "sourceId" is a required field
- "sinkId" is a required field
- "fileOpenedAt" is a required field
`), strings.TrimSpace(err.Error()))
	}
}

func TestFileKey_String(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "123/456/my-source/my-sink/2006-01-02T15:04:05.000Z", testFileKey().String())
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
	localStorage := localModel.File{
		Dir:            "my-dir",
		Compression:    compression.NewConfig(),
		DiskSync:       writesync.NewConfig(),
		DiskAllocation: diskalloc.NewConfig(),
	}
	stagingStorage := stagingModel.File{
		Provider:    "foo",
		Compression: compression.NewConfig(),
		Expiration:  utctime.MustParse("2006-01-02T15:04:05.000Z"),
	}
	targetStorage := targetModel.Target{
		Provider: "foo",
	}
	volumeAssignment := assignment.Assignment{
		Config: assignment.Config{
			Count:          2,
			PreferredTypes: []string{"foo", "bar"},
		},
		Volumes: []volume.ID{"my-volume-1", "my-volume-2"},
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
- "branchId" is a required field
- "sourceId" is a required field
- "sinkId" is a required field
- "fileOpenedAt" is a required field
- "type" is a required field
- "state" is a required field
- "columns" is a required field
- "assignment.config.count" is a required field
- "assignment.config.preferredTypes" is a required field
- "assignment.volumes" must contain at least 1 item
- "local.dir" is a required field
- "local.compression.type" is a required field
- "local.diskSync.mode" is a required field
- "local.diskAllocation.static" is a required field
- "local.diskAllocation.relative" must be 100 or greater
- "staging.provider" is a required field
- "staging.compression" is a required field
- "staging.expiration" is a required field
- "target.provider" is a required field
`,
			Value: File{},
		},
		{
			Name:          "empty columns",
			ExpectedError: ` "columns" must contain at least 1 item`,
			Value: File{
				FileKey:        testFileKey(),
				Type:           FileTypeCSV,
				State:          FileWriting,
				Columns:        column.Columns{},
				Assignment:     volumeAssignment,
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
					column.UUID{},
					column.Headers{},
					column.Body{},
				},
				Assignment:     volumeAssignment,
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
				ClosingAt: ptr.Ptr(utctime.MustParse("2006-01-02T15:04:05.000Z")),
				Columns: column.Columns{
					column.UUID{},
					column.Headers{},
					column.Body{},
				},
				Assignment:     volumeAssignment,
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
				ClosingAt:   ptr.Ptr(utctime.MustParse("2006-01-02T15:04:05.000Z")),
				ImportingAt: ptr.Ptr(utctime.MustParse("2006-01-02T16:04:05.000Z")),
				Columns: column.Columns{
					column.UUID{},
					column.Headers{},
					column.Body{},
				},
				Assignment:     volumeAssignment,
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
				ClosingAt:   ptr.Ptr(utctime.MustParse("2006-01-02T15:04:05.000Z")),
				ImportingAt: ptr.Ptr(utctime.MustParse("2006-01-02T16:04:05.000Z")),
				ImportedAt:  ptr.Ptr(utctime.MustParse("2006-01-02T17:04:05.000Z")),
				Columns: column.Columns{
					column.UUID{},
					column.Headers{},
					column.Body{},
				},
				Assignment:     volumeAssignment,
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
		} else if assert.Error(t, err, tc.Name) {
			assert.Equal(t, strings.TrimSpace(tc.ExpectedError), strings.TrimSpace(err.Error()), tc.Name)
		}
	}
}

func testFileKey() FileKey {
	return FileKey{
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
			OpenedAt: utctime.MustParse("2006-01-02T15:04:05.000Z"),
		},
	}
}
