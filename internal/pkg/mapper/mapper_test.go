package mapper

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/fileloader"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func TestMappers_ForEach_StopOnFailure(t *testing.T) {
	t.Parallel()
	callOrder := make([]string, 0)
	mappers := Mappers{`1`, `2`, `3`, `4`, `5`}
	err := mappers.ForEach(true, func(mapper interface{}) error {
		callOrder = append(callOrder, mapper.(string))
		return fmt.Errorf(`error %s`, mapper.(string))
	})
	assert.Error(t, err)
	assert.Equal(t, `error 1`, err.Error())
	assert.Equal(t, []string{`1`}, callOrder)
}

func TestMappers_ForEach_DontStopOnFailure(t *testing.T) {
	t.Parallel()
	callOrder := make([]string, 0)
	mappers := Mappers{`1`, `2`, `3`, `4`, `5`}
	err := mappers.ForEach(false, func(mapper interface{}) error {
		callOrder = append(callOrder, mapper.(string))
		return fmt.Errorf(`error %s`, mapper.(string))
	})
	assert.Error(t, err)
	assert.Equal(t, "- error 1\n- error 2\n- error 3\n- error 4\n- error 5", err.Error())
	assert.Equal(t, []string{`1`, `2`, `3`, `4`, `5`}, callOrder)
}

func TestMappers_ForEachReverse_StopOnFailure(t *testing.T) {
	t.Parallel()
	callOrder := make([]string, 0)
	mappers := Mappers{`1`, `2`, `3`, `4`, `5`}
	err := mappers.ForEachReverse(true, func(mapper interface{}) error {
		callOrder = append(callOrder, mapper.(string))
		return fmt.Errorf(`error %s`, mapper.(string))
	})
	assert.Error(t, err)
	assert.Equal(t, `error 5`, err.Error())
	assert.Equal(t, []string{`5`}, callOrder)
}

func TestMappers_ForEachReverse_DontStopOnFailure(t *testing.T) {
	t.Parallel()
	callOrder := make([]string, 0)
	mappers := Mappers{`1`, `2`, `3`, `4`, `5`}
	err := mappers.ForEachReverse(false, func(mapper interface{}) error {
		callOrder = append(callOrder, mapper.(string))
		return fmt.Errorf(`error %s`, mapper.(string))
	})
	assert.Error(t, err)
	assert.Equal(t, "- error 5\n- error 4\n- error 3\n- error 2\n- error 1", err.Error())
	assert.Equal(t, []string{`5`, `4`, `3`, `2`, `1`}, callOrder)
}

func TestMapper_LoadLocalFile_DefaultHandler(t *testing.T) {
	t.Parallel()
	expectedLogs := `
INFO  Handler 1
INFO  Handler 2
INFO  Handler 3
INFO  Default Handler
`
	invokeLoadLocalFile(
		t,
		filesystem.NewFileDef(`foo.txt`),
		filesystem.NewRawFile(`file.txt`, `default`),
		expectedLogs,
	)
}

func TestMapper_LoadLocalFile_CustomHandler(t *testing.T) {
	t.Parallel()
	expectedLogs := `
INFO  Handler 1
INFO  Handler 2
`
	invokeLoadLocalFile(
		t,
		filesystem.NewFileDef(`file2.txt`),
		filesystem.NewRawFile(`file2.txt`, `handler2`),
		expectedLogs,
	)
}

type testFileLoadMapper struct {
	callback func(def *filesystem.FileDef, fileType filesystem.FileType, next fileloader.Handler) (filesystem.File, error)
}

func (m *testFileLoadMapper) LoadLocalFile(def *filesystem.FileDef, fileType filesystem.FileType, next fileloader.Handler) (filesystem.File, error) {
	return m.callback(def, fileType, next)
}

func invokeLoadLocalFile(t *testing.T, input *filesystem.FileDef, expected filesystem.File, expectedLogs string) {
	t.Helper()
	logger := log.NewDebugLogger()

	// File load handlers
	handler1 := func(def *filesystem.FileDef, fileType filesystem.FileType, next fileloader.Handler) (filesystem.File, error) {
		// Match file path "file1.txt"
		logger.Info(`Handler 1`)
		if def.Path() == "file1.txt" {
			return filesystem.NewRawFile("file1.txt", "handler1"), nil
		}
		return next(def, fileType)
	}
	handler2 := func(def *filesystem.FileDef, fileType filesystem.FileType, next fileloader.Handler) (filesystem.File, error) {
		// Match file path "file2.txt"
		logger.Info(`Handler 2`)
		if def.Path() == "file2.txt" {
			return filesystem.NewRawFile("file2.txt", "handler2"), nil
		}
		return next(def, fileType)
	}
	handler3 := func(def *filesystem.FileDef, fileType filesystem.FileType, next fileloader.Handler) (filesystem.File, error) {
		// Match file path "file3.txt"
		logger.Info(`Handler 3`)
		if def.Path() == "file3.txt" {
			return filesystem.NewRawFile("file3.txt", "handler3"), nil
		}
		return next(def, fileType)
	}
	defaultHandler := func(def *filesystem.FileDef, _ filesystem.FileType) (filesystem.File, error) {
		logger.Info(`Default Handler`)
		return filesystem.NewRawFile("file.txt", "default"), nil
	}

	// Create mapper
	mapper := New()
	mapper.AddMapper(
		&testFileLoadMapper{callback: handler1},
		&testFileLoadMapper{callback: handler2},
		&testFileLoadMapper{callback: handler3},
	)

	// Invoke
	output, err := mapper.NewFileLoader(defaultHandler).ReadRawFile(input)
	assert.NoError(t, err)
	assert.Equal(t, expected, output)
	assert.Equal(t, strings.TrimLeft(expectedLogs, "\n"), logger.AllMessages())
}
