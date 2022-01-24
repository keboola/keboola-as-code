package fileloader_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/fileloader"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type myStruct struct {
	Field1   string                 `json:"field1" mytag:"field"`
	Field2   string                 `json:"field2" mytag:"field"`
	FooField string                 `json:"foo"`
	Map      *orderedmap.OrderedMap `mytag:"map"`
	Content  string                 `mytag:"content"`
}

func TestLocalFilesystem(t *testing.T) {
	t.Parallel()
	createFs := func() (filesystem.Fs, log.DebugLogger) {
		logger := log.NewDebugLogger()
		fs, err := aferofs.NewLocalFs(logger, t.TempDir(), filesystem.Join("my", "dir"))
		assert.NoError(t, err)
		return fs, logger
	}
	cases := &testCases{createFs}
	cases.runTests(t)
}

func TestMemoryFilesystem(t *testing.T) {
	t.Parallel()
	createFs := func() (filesystem.Fs, log.DebugLogger) {
		logger := log.NewDebugLogger()
		fs, err := aferofs.NewMemoryFs(logger, filesystem.Join("my", "dir"))
		assert.NoError(t, err)
		return fs, logger
	}
	cases := &testCases{createFs}
	cases.runTests(t)
}

type testCases struct {
	createFs func() (filesystem.Fs, log.DebugLogger)
}

func (tc *testCases) runTests(t *testing.T) {
	t.Helper()
	// Call all Test* methods
	tp := reflect.TypeOf(tc)
	prefix := `Test`
	for i := 0; i < tp.NumMethod(); i++ {
		method := tp.Method(i)
		if strings.HasPrefix(method.Name, prefix) {
			fs, logger := tc.createFs()
			testName := strings.TrimPrefix(method.Name, prefix)
			t.Run(testName, func(t *testing.T) {
				t.Parallel()
				reflect.ValueOf(tc).MethodByName(method.Name).Call([]reflect.Value{
					reflect.ValueOf(t),
					reflect.ValueOf(fs),
					reflect.ValueOf(logger),
				})
			})
		}
	}
}

func (*testCases) TestFileLoader_ReadFile(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filePath, "foo\n")))

	// Read
	logger.Truncate()
	file, err := fileloader.New(fs).ReadFile(filesystem.NewFileDef(filePath))
	assert.NoError(t, err)
	assert.NotNil(t, file)
	assert.Equal(t, "foo\n", file.Content)
	assert.Equal(t, `DEBUG  Loaded "file.txt"`, strings.TrimSpace(logger.AllMessages()))
}

func (*testCases) TestFileLoader_ReadFile_NotFound(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	filePath := "file.txt"
	file, err := fileloader.New(fs).ReadFile(filesystem.NewFileDef(filePath))
	assert.Error(t, err)
	assert.Nil(t, file)
	assert.True(t, strings.HasPrefix(err.Error(), `missing file "file.txt"`))
	assert.Equal(t, "", logger.AllMessages())
}

func (*testCases) TestFileLoader_ReadJsonFile(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filePath, `{"foo": "bar"}`)))

	// Read
	logger.Truncate()
	file, err := fileloader.New(fs).ReadJsonFile(filesystem.NewFileDef(filePath))
	assert.NoError(t, err)
	assert.NotNil(t, file)
	assert.Equal(t, `{"foo":"bar"}`, json.MustEncodeString(file.Content, false))
	assert.Equal(t, `DEBUG  Loaded "file.txt"`, strings.TrimSpace(logger.AllMessages()))
}

func (*testCases) TestFileLoader_ReadJsonFileTo(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filePath, `{"foo": "bar"}`)))

	// Read
	logger.Truncate()
	target := &myStruct{}
	file, err := fileloader.New(fs).ReadJsonFileTo(filesystem.NewFileDef(filePath), target)
	assert.Equal(t, `{"foo": "bar"}`, file.Content)
	assert.NoError(t, err)
	assert.Equal(t, `bar`, target.FooField)
	assert.Equal(t, `DEBUG  Loaded "file.txt"`, strings.TrimSpace(logger.AllMessages()))
}

func (*testCases) TestFileLoader_ReadJsonFileTo_Invalid(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filePath, `{"foo":`)))

	// Read
	logger.Truncate()
	target := &myStruct{}
	_, err := fileloader.New(fs).ReadJsonFileTo(filesystem.NewFileDef(filePath), target)
	assert.Error(t, err)
	expectedError := `
file "file.txt" is invalid:
  - unexpected end of JSON input, offset: 7
`
	assert.Equal(t, strings.TrimSpace(expectedError), err.Error())
}

func (*testCases) TestFileLoader_ReadJsonFile_Invalid(t *testing.T, fs filesystem.Fs, _ log.DebugLogger) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filePath, `{"foo":`)))

	// Read
	file, err := fileloader.New(fs).ReadJsonFile(filesystem.NewFileDef(filePath))
	assert.Error(t, err)
	assert.Nil(t, file)
	expectedError := `
file "file.txt" is invalid:
  - unexpected end of JSON input, offset: 7
`
	assert.Equal(t, strings.TrimSpace(expectedError), err.Error())
}

func (*testCases) TestFileLoader_ReadJsonFieldsTo(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filePath, `{"field1": "foo", "field2": "bar"}`)))

	// Read
	logger.Truncate()
	target := &myStruct{}
	file, tagFound, err := fileloader.New(fs).ReadJsonFieldsTo(filesystem.NewFileDef(filePath), target, `mytag:field`)
	assert.NoError(t, err)
	assert.True(t, tagFound)
	assert.NotNil(t, file)
	assert.Equal(t, `{"field1":"foo","field2":"bar"}`, json.MustEncodeString(file.Content, false))
	assert.Equal(t, `foo`, target.Field1)
	assert.Equal(t, `bar`, target.Field2)
	assert.Equal(t, `DEBUG  Loaded "file.txt"`, strings.TrimSpace(logger.AllMessages()))
}

func (*testCases) TestFileLoader_ReadJsonMapTo(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filePath, `{"field1": "foo", "field2": "bar"}`)))

	// Read
	logger.Truncate()
	target := &myStruct{}
	file, tagFound, err := fileloader.New(fs).ReadJsonMapTo(filesystem.NewFileDef(filePath), target, `mytag:map`)
	assert.NoError(t, err)
	assert.True(t, tagFound)
	assert.NotNil(t, file)
	assert.Equal(t, `{"field1":"foo","field2":"bar"}`, json.MustEncodeString(file.Content, false))
	assert.Equal(t, `{"field1":"foo","field2":"bar"}`, json.MustEncodeString(target.Map, false))
	assert.Equal(t, `DEBUG  Loaded "file.txt"`, strings.TrimSpace(logger.AllMessages()))
}

func (*testCases) TestFileLoader_ReadFileContentTo(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filePath, `{"field1": "foo", "field2": "bar"}`)))

	// Read
	logger.Truncate()
	target := &myStruct{}
	file, tagFound, err := fileloader.New(fs).ReadFileContentTo(filesystem.NewFileDef(filePath), target, `mytag:content`)
	assert.NoError(t, err)
	assert.True(t, tagFound)
	assert.NotNil(t, file)
	assert.Equal(t, `{"field1": "foo", "field2": "bar"}`, file.Content)
	assert.Equal(t, `{"field1": "foo", "field2": "bar"}`, target.Content)
	assert.Equal(t, `DEBUG  Loaded "file.txt"`, strings.TrimSpace(logger.AllMessages()))
}
