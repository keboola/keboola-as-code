package mapper

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestMappers_ForEach_StopOnFailure(t *testing.T) {
	t.Parallel()
	callOrder := make([]string, 0)
	mappers := Mappers{`1`, `2`, `3`, `4`, `5`}
	err := mappers.ForEach(true, func(mapper any) error {
		callOrder = append(callOrder, mapper.(string))
		return errors.Errorf(`error %s`, mapper.(string))
	})
	require.Error(t, err)
	assert.Equal(t, `error 1`, err.Error())
	assert.Equal(t, []string{`1`}, callOrder)
}

func TestMappers_ForEach_DontStopOnFailure(t *testing.T) {
	t.Parallel()
	callOrder := make([]string, 0)
	mappers := Mappers{`1`, `2`, `3`, `4`, `5`}
	err := mappers.ForEach(false, func(mapper any) error {
		callOrder = append(callOrder, mapper.(string))
		return errors.Errorf(`error %s`, mapper.(string))
	})
	require.Error(t, err)
	assert.Equal(t, "- error 1\n- error 2\n- error 3\n- error 4\n- error 5", err.Error())
	assert.Equal(t, []string{`1`, `2`, `3`, `4`, `5`}, callOrder)
}

func TestMappers_ForEachReverse_StopOnFailure(t *testing.T) {
	t.Parallel()
	callOrder := make([]string, 0)
	mappers := Mappers{`1`, `2`, `3`, `4`, `5`}
	err := mappers.ForEachReverse(true, func(mapper any) error {
		callOrder = append(callOrder, mapper.(string))
		return errors.Errorf(`error %s`, mapper.(string))
	})
	require.Error(t, err)
	assert.Equal(t, `error 5`, err.Error())
	assert.Equal(t, []string{`5`}, callOrder)
}

func TestMappers_ForEachReverse_DontStopOnFailure(t *testing.T) {
	t.Parallel()
	callOrder := make([]string, 0)
	mappers := Mappers{`1`, `2`, `3`, `4`, `5`}
	err := mappers.ForEachReverse(false, func(mapper any) error {
		callOrder = append(callOrder, mapper.(string))
		return errors.Errorf(`error %s`, mapper.(string))
	})
	require.Error(t, err)
	assert.Equal(t, "- error 5\n- error 4\n- error 3\n- error 2\n- error 1", err.Error())
	assert.Equal(t, []string{`5`, `4`, `3`, `2`, `1`}, callOrder)
}

func TestMapper_LoadLocalFile_DefaultHandler(t *testing.T) {
	t.Parallel()
	expectedLogs := `
{"level":"info","message":"Handler 1"}
{"level":"info","message":"Handler 2"}
{"level":"info","message":"Handler 3"}
{"level":"debug","message":"Loaded \"file.txt\""}
`
	invokeLoadLocalFile(
		t,
		filesystem.NewFileDef(`file.txt`),
		filesystem.NewRawFile(`file.txt`, `default`),
		expectedLogs,
	)
}

func TestMapper_LoadLocalFile_CustomHandler(t *testing.T) {
	t.Parallel()
	expectedLogs := `
{"level":"info","message":"Handler 1"}
{"level":"info","message":"Handler 2"}
`
	invokeLoadLocalFile(
		t,
		filesystem.NewFileDef(`file2.txt`),
		filesystem.NewRawFile(`file2.txt`, `handler2`),
		expectedLogs,
	)
}

type testFileLoadMapper struct {
	callback func(ctx context.Context, def *filesystem.FileDef, fileType filesystem.FileType, next filesystem.LoadHandler) (filesystem.File, error)
}

func (m *testFileLoadMapper) LoadLocalFile(ctx context.Context, def *filesystem.FileDef, fileType filesystem.FileType, next filesystem.LoadHandler) (filesystem.File, error) {
	return m.callback(ctx, def, fileType, next)
}

func invokeLoadLocalFile(t *testing.T, input *filesystem.FileDef, expected filesystem.File, expectedLogs string) {
	t.Helper()
	logger := log.NewDebugLogger()
	ctx := context.Background()

	// File load handlers
	handler1 := func(ctx context.Context, def *filesystem.FileDef, fileType filesystem.FileType, next filesystem.LoadHandler) (filesystem.File, error) {
		// Match file path "file1.txt"
		logger.Info(context.Background(), `Handler 1`)
		if def.Path() == "file1.txt" {
			return filesystem.NewRawFile("file1.txt", "handler1"), nil
		}
		return next(ctx, def, fileType)
	}
	handler2 := func(ctx context.Context, def *filesystem.FileDef, fileType filesystem.FileType, next filesystem.LoadHandler) (filesystem.File, error) {
		// Match file path "file2.txt"
		logger.Info(context.Background(), `Handler 2`)
		if def.Path() == "file2.txt" {
			return filesystem.NewRawFile("file2.txt", "handler2"), nil
		}
		return next(ctx, def, fileType)
	}
	handler3 := func(ctx context.Context, def *filesystem.FileDef, fileType filesystem.FileType, next filesystem.LoadHandler) (filesystem.File, error) {
		// Match file path "file3.txt"
		logger.Info(context.Background(), `Handler 3`)
		if def.Path() == "file3.txt" {
			return filesystem.NewRawFile("file3.txt", "handler3"), nil
		}
		return next(ctx, def, fileType)
	}

	// Default file
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(logger))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile("file.txt", "default")))
	logger.Truncate()

	// Create mapper
	mapper := New()
	mapper.AddMapper(
		&testFileLoadMapper{callback: handler1},
		&testFileLoadMapper{callback: handler2},
		&testFileLoadMapper{callback: handler3},
	)

	// Invoke
	output, err := mapper.NewFileLoader(fs).ReadRawFile(ctx, input)
	require.NoError(t, err)
	assert.Equal(t, expected, output)
	logger.AssertJSONMessages(t, expectedLogs)
}
