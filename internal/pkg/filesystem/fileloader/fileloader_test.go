// nolint forbidigo
package fileloader_test

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/fileloader"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

type myStruct struct {
	Field1   string                 `json:"field1" yaml:"field1" mytag:"field"`
	Field2   string                 `json:"field2" yaml:"field2" mytag:"field"`
	FooField string                 `json:"foo" yaml:"foo"`
	Map      *orderedmap.OrderedMap `mytag:"map"`
	Content  string                 `mytag:"content"`
}

func TestLocalFilesystem(t *testing.T) {
	t.Parallel()
	createFs := func() (filesystem.Fs, log.DebugLogger) {
		logger := log.NewDebugLogger()
		fs, err := aferofs.NewLocalFs(t.TempDir(), filesystem.WithLogger(logger), filesystem.WithWorkingDir(filesystem.Join("my", "dir")))
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
		fs := aferofs.NewMemoryFs(filesystem.WithLogger(logger), filesystem.WithWorkingDir(filesystem.Join("my", "dir")))
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
	tp := reflect.TypeFor[*testCases]()
	prefix := `Test`
	for i := range tp.NumMethod() {
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

func (*testCases) TestFileLoader_ReadRawFile(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(context.Background(), filesystem.NewRawFile(filePath, "foo\n")))

	// Read
	logger.Truncate()
	file, err := fs.FileLoader().ReadRawFile(context.Background(), filesystem.NewFileDef(filePath))
	assert.NoError(t, err)
	assert.NotNil(t, file)
	assert.Equal(t, "foo\n", file.Content)
	logger.AssertJSONMessages(t, `{"level":"debug","message":"Loaded \"file.txt\""}`)
}

func (*testCases) TestFileLoader_ReadRawFile_NotFound(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	filePath := "file.txt"
	file, err := fs.FileLoader().ReadRawFile(context.Background(), filesystem.NewFileDef(filePath))
	assert.Error(t, err)
	assert.Nil(t, file)
	assert.True(t, strings.HasPrefix(err.Error(), `missing file "file.txt"`))
	assert.Equal(t, "", logger.AllMessages())
}

func (*testCases) TestFileLoader_ReadJsonFile(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(context.Background(), filesystem.NewRawFile(filePath, `{"foo": "bar"}`)))

	// Read
	logger.Truncate()
	file, err := fs.FileLoader().ReadJSONFile(context.Background(), filesystem.NewFileDef(filePath))
	assert.NoError(t, err)
	assert.NotNil(t, file)
	assert.Equal(t, `{"foo":"bar"}`, json.MustEncodeString(file.Content, false))
	logger.AssertJSONMessages(t, `{"level":"debug","message":"Loaded \"file.txt\""}`)
}

func (*testCases) TestFileLoader_ReadJsonFileTo(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(context.Background(), filesystem.NewRawFile(filePath, `{"foo": "bar"}`)))

	// Read
	logger.Truncate()
	target := &myStruct{}
	file, err := fs.FileLoader().ReadJSONFileTo(context.Background(), filesystem.NewFileDef(filePath), target)
	assert.Equal(t, `{"foo": "bar"}`, file.Content)
	assert.NoError(t, err)
	assert.Equal(t, `bar`, target.FooField)
	logger.AssertJSONMessages(t, `{"level":"debug","message":"Loaded \"file.txt\""}`)
}

func (*testCases) TestFileLoader_ReadJsonFileTo_Invalid(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(context.Background(), filesystem.NewRawFile(filePath, `{"foo":`)))

	// Read
	logger.Truncate()
	target := &myStruct{}
	_, err := fs.FileLoader().ReadJSONFileTo(context.Background(), filesystem.NewFileDef(filePath), target)
	assert.Error(t, err)
	expectedError := `
file "file.txt" is invalid:
- unexpected end of JSON input, offset: 7
`
	assert.Equal(t, strings.TrimSpace(expectedError), err.Error())
}

func (*testCases) TestFileLoader_ReadJsonnetFile(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(context.Background(), filesystem.NewRawFile(filePath, `{foo: "bar"}`)))

	// Read
	logger.Truncate()
	file, err := fs.FileLoader().ReadJsonnetFile(context.Background(), filesystem.NewFileDef(filePath))
	assert.NoError(t, err)
	assert.NotNil(t, file)
	jsonnetCode, err := jsonnet.FormatNode(file.Content)
	assert.NoError(t, err)
	assert.Equal(t, "{ foo: \"bar\" }\n", jsonnetCode)
	logger.AssertJSONMessages(t, `{"level":"debug","message":"Loaded \"file.txt\""}`)
}

func (*testCases) TestFileLoader_ReadJsonnetFileTo(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(context.Background(), filesystem.NewRawFile(filePath, `{foo: "bar"}`)))

	// Read
	logger.Truncate()
	target := &myStruct{}
	file, err := fs.FileLoader().ReadJsonnetFileTo(context.Background(), filesystem.NewFileDef(filePath), target)
	assert.NotEmpty(t, file.Content)
	assert.NoError(t, err)
	assert.Equal(t, `bar`, target.FooField)
	logger.AssertJSONMessages(t, `{"level":"debug","message":"Loaded \"file.txt\""}`)
}

func (*testCases) TestFileLoader_ReadJsonnetFileTo_Invalid(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(context.Background(), filesystem.NewRawFile(filePath, `{foo:`)))

	// Read
	logger.Truncate()
	target := &myStruct{}
	_, err := fs.FileLoader().ReadJsonnetFileTo(context.Background(), filesystem.NewFileDef(filePath), target)
	assert.Error(t, err)
	expectedError := `
file "file.txt" is invalid:
- cannot parse jsonnet: file.txt:1:6 Unexpected end of file
`
	assert.Equal(t, strings.TrimSpace(expectedError), err.Error())
}

func (*testCases) TestFileLoader_ReadJsonFile_Invalid(t *testing.T, fs filesystem.Fs, _ log.DebugLogger) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(context.Background(), filesystem.NewRawFile(filePath, `{"foo":`)))

	// Read
	file, err := fs.FileLoader().ReadJSONFile(context.Background(), filesystem.NewFileDef(filePath))
	assert.Error(t, err)
	assert.Nil(t, file)
	expectedError := `
file "file.txt" is invalid:
- unexpected end of JSON input, offset: 7
`
	assert.Equal(t, strings.TrimSpace(expectedError), err.Error())
}

func (*testCases) TestFileLoader_ReadJSONFieldsTo(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(context.Background(), filesystem.NewRawFile(filePath, `{"field1": "foo", "field2": "bar"}`)))

	// Read
	logger.Truncate()
	target := &myStruct{}
	file, tagFound, err := fs.FileLoader().ReadJSONFieldsTo(context.Background(), filesystem.NewFileDef(filePath), target, `mytag:field`)
	assert.NoError(t, err)
	assert.True(t, tagFound)
	assert.NotNil(t, file)
	assert.Equal(t, `{"field1":"foo","field2":"bar"}`, json.MustEncodeString(file.Content, false))
	assert.Equal(t, `foo`, target.Field1)
	assert.Equal(t, `bar`, target.Field2)
	logger.AssertJSONMessages(t, `{"level":"debug","message":"Loaded \"file.txt\""}`)
}

func (*testCases) TestFileLoader_ReadJsonMapTo(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(context.Background(), filesystem.NewRawFile(filePath, `{"field1": "foo", "field2": "bar"}`)))

	// Read
	logger.Truncate()
	target := &myStruct{}
	file, tagFound, err := fs.FileLoader().ReadJSONMapTo(context.Background(), filesystem.NewFileDef(filePath), target, `mytag:map`)
	assert.NoError(t, err)
	assert.True(t, tagFound)
	assert.NotNil(t, file)
	assert.Equal(t, `{"field1":"foo","field2":"bar"}`, json.MustEncodeString(file.Content, false))
	assert.Equal(t, `{"field1":"foo","field2":"bar"}`, json.MustEncodeString(target.Map, false))
	logger.AssertJSONMessages(t, `{"level":"debug","message":"Loaded \"file.txt\""}`)
}

func (*testCases) TestFileLoader_ReadYamlFile(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	// Create file
	filePath := "file.yaml"
	assert.NoError(t, fs.WriteFile(context.Background(), filesystem.NewRawFile(filePath, `foo: bar`)))

	// Read
	logger.Truncate()
	file, err := fs.FileLoader().ReadYamlFile(context.Background(), filesystem.NewFileDef(filePath))
	assert.NoError(t, err)
	assert.NotNil(t, file)
	assert.Equal(t, `{"foo":"bar"}`, json.MustEncodeString(file.Content, false))
	logger.AssertJSONMessages(t, `{"level":"debug","message":"Loaded \"file.yaml\""}`)
}

func (*testCases) TestFileLoader_ReadYamlFileTo(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(context.Background(), filesystem.NewRawFile(filePath, "foo: bar")))

	// Read
	logger.Truncate()
	target := &myStruct{}
	file, err := fs.FileLoader().ReadYamlFileTo(context.Background(), filesystem.NewFileDef(filePath), target)
	assert.NoError(t, err)
	assert.Equal(t, `foo: bar`, file.Content)
	assert.Equal(t, `bar`, target.FooField)
	logger.AssertJSONMessages(t, `{"level":"debug","message":"Loaded \"file.txt\""}`)
}

func (*testCases) TestFileLoader_ReadYamlFile_Invalid(t *testing.T, fs filesystem.Fs, _ log.DebugLogger) {
	// Create file
	filePath := "file.yaml"
	assert.NoError(t, fs.WriteFile(context.Background(), filesystem.NewRawFile(filePath, ":\n")))

	// Read
	file, err := fs.FileLoader().ReadYamlFile(context.Background(), filesystem.NewFileDef(filePath))
	assert.Error(t, err)
	assert.Nil(t, file)
	expectedError := `
file "file.yaml" is invalid:
- yaml: did not find expected key
`
	assert.Equal(t, strings.TrimSpace(expectedError), err.Error())
}

func (*testCases) TestFileLoader_ReadYamlFieldsTo(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	// Create file
	filePath := "file.yaml"
	assert.NoError(t, fs.WriteFile(context.Background(), filesystem.NewRawFile(filePath, "field1: foo\nfield2: bar\n")))

	// Read
	logger.Truncate()
	target := &myStruct{}
	file, tagFound, err := fs.FileLoader().ReadYamlFieldsTo(context.Background(), filesystem.NewFileDef(filePath), target, `mytag:field`)
	assert.NoError(t, err)
	assert.True(t, tagFound)
	assert.NotNil(t, file)
	assert.Equal(t, `{"field1":"foo","field2":"bar"}`, json.MustEncodeString(file.Content, false))
	assert.Equal(t, `foo`, target.Field1)
	assert.Equal(t, `bar`, target.Field2)
	logger.AssertJSONMessages(t, `{"level":"debug","message":"Loaded \"file.yaml\""}`)
}

func (*testCases) TestFileLoader_ReadYamlMapTo(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	// Create file
	filePath := "file.yaml"
	assert.NoError(t, fs.WriteFile(context.Background(), filesystem.NewRawFile(filePath, "field1: foo\nfield2: bar\n")))

	// Read
	logger.Truncate()
	target := &myStruct{}
	file, tagFound, err := fs.FileLoader().ReadYamlMapTo(context.Background(), filesystem.NewFileDef(filePath), target, `mytag:map`)
	assert.NoError(t, err)
	assert.True(t, tagFound)
	assert.NotNil(t, file)
	assert.Equal(t, `{"field1":"foo","field2":"bar"}`, json.MustEncodeString(file.Content, false))
	assert.Equal(t, `{"field1":"foo","field2":"bar"}`, json.MustEncodeString(target.Map, false))
	logger.AssertJSONMessages(t, `{"level":"debug","message":"Loaded \"file.yaml\""}`)
}

func (*testCases) TestFileLoader_ReadFileContentTo(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(context.Background(), filesystem.NewRawFile(filePath, `{"field1": "foo", "field2": "bar"}`)))

	// Read
	logger.Truncate()
	target := &myStruct{}
	file, tagFound, err := fs.FileLoader().ReadFileContentTo(context.Background(), filesystem.NewFileDef(filePath), target, `mytag:content`)
	assert.NoError(t, err)
	assert.True(t, tagFound)
	assert.NotNil(t, file)
	assert.Equal(t, `{"field1": "foo", "field2": "bar"}`, file.Content)
	assert.Equal(t, `{"field1": "foo", "field2": "bar"}`, target.Content)
	logger.AssertJSONMessages(t, `{"level":"debug","message":"Loaded \"file.txt\""}`)
}

func (*testCases) TestFileLoader_ReadSubDirs(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	ctx := context.Background()
	// Create dirs and kbcdir files
	assert.NoError(t, fs.Mkdir(ctx, "dir1"))
	assert.NoError(t, fs.Mkdir(ctx, "dir2"))
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filepath.Join("dir2", fileloader.KbcDirFileName), `{"foo": "bar"}`)))
	assert.NoError(t, fs.Mkdir(ctx, "dir3"))
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filepath.Join("dir3", fileloader.KbcDirFileName), `{"isIgnored": false}`)))
	assert.NoError(t, fs.Mkdir(ctx, "dir4"))
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filepath.Join("dir4", fileloader.KbcDirFileName), `{"isIgnored": true}`)))

	dirs, err := fs.FileLoader().ReadSubDirs(ctx, fs, ".")
	assert.NoError(t, err)
	assert.Equal(t, []string{"dir1", "dir2", "dir3"}, dirs)
}

func (*testCases) TestFileLoader_IsIgnored(t *testing.T, fs filesystem.Fs, logger log.DebugLogger) {
	ctx := context.Background()
	// Create dirs and kbcdir files
	assert.NoError(t, fs.Mkdir(ctx, "dir1"))
	assert.NoError(t, fs.Mkdir(ctx, "dir2"))
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filepath.Join("dir2", fileloader.KbcDirFileName), `{"foo": "bar"}`)))
	assert.NoError(t, fs.Mkdir(ctx, "dir3"))
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filepath.Join("dir3", fileloader.KbcDirFileName), `{"isIgnored": false}`)))
	assert.NoError(t, fs.Mkdir(ctx, "dir4"))
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filepath.Join("dir4", fileloader.KbcDirFileName), `{"isIgnored": true}`)))

	isIgnored, err := fs.FileLoader().IsIgnored(ctx, "dir1")
	assert.NoError(t, err)
	assert.False(t, isIgnored)

	isIgnored, err = fs.FileLoader().IsIgnored(ctx, "dir2")
	assert.NoError(t, err)
	assert.False(t, isIgnored)

	isIgnored, err = fs.FileLoader().IsIgnored(ctx, "dir3")
	assert.NoError(t, err)
	assert.False(t, isIgnored)

	isIgnored, err = fs.FileLoader().IsIgnored(ctx, "dir4")
	assert.NoError(t, err)
	assert.True(t, isIgnored)
}
