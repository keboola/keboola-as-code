package filesystem_test

import (
	"io"
	iofs "io/fs"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/iancoleman/orderedmap"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	. "github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type myStruct struct {
	Field1   string                 `json:"field1" mytag:"field"`
	Field2   string                 `json:"field2" mytag:"field"`
	FooField string                 `json:"foo"`
	Map      *orderedmap.OrderedMap `mytag:"map"`
	Content  string                 `mytag:"content"`
}

func TestLocalFilesystem(t *testing.T) {
	createFs := func() (model.Filesystem, *utils.Writer) {
		logger, out := utils.NewDebugLogger()
		projectDir := t.TempDir()
		fs, err := NewLocalFsFromProjectDir(logger, projectDir, "/")
		assert.NoError(t, err)
		return fs, out
	}
	cases := &testCases{createFs}
	cases.runTests(t)
}

func TestMemoryFilesystem(t *testing.T) {
	createFs := func() (model.Filesystem, *utils.Writer) {
		logger, out := utils.NewDebugLogger()
		fs, err := NewMemoryFs(logger, "/")
		assert.NoError(t, err)
		return fs, out
	}
	cases := &testCases{createFs}
	cases.runTests(t)
}

type testCases struct {
	createFs func() (model.Filesystem, *utils.Writer)
}

func (tc *testCases) runTests(t *testing.T) {
	t.Helper()
	// Call all Test* methods
	tp := reflect.TypeOf(tc)
	prefix := `Test`
	for i := 0; i < tp.NumMethod(); i++ {
		method := tp.Method(i)
		if strings.HasPrefix(method.Name, prefix) {
			fs, log := tc.createFs()
			testName := strings.TrimPrefix(method.Name, prefix)
			t.Run(testName, func(t *testing.T) {
				reflect.ValueOf(tc).MethodByName(method.Name).Call([]reflect.Value{
					reflect.ValueOf(t),
					reflect.ValueOf(fs),
					reflect.ValueOf(log),
				})
			})
		}
	}
}

func (*testCases) TestApiName(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.NotEmpty(t, fs.ApiName())
}

func (*testCases) TestBasePath(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.NotEmpty(t, fs.BasePath())
}

func (*testCases) TestWorkingDir(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.Equal(t, "/", fs.WorkingDir())
}

func (*testCases) TestSetLogger(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	logger := zap.NewNop().Sugar()
	assert.NotPanics(t, func() {
		fs.SetLogger(logger)
	})
}

func (*testCases) TestWalk(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.NoError(t, fs.Mkdir("my/dir1"))
	assert.NoError(t, fs.WriteFile(model.CreateFile("my/dir2/file.txt", "foo\n")))

	paths := make([]string, 0)
	root := "."
	err := fs.Walk(root, func(path string, info iofs.FileInfo, err error) error {
		// Skip root
		if root == path {
			return nil
		}

		assert.NoError(t, err)
		paths = append(paths, path)
		return err
	})
	assert.NoError(t, err)
	assert.Equal(t, []string{
		`my`,
		`my/dir1`,
		`my/dir2`,
		`my/dir2/file.txt`,
	}, paths)
}

func (*testCases) TestGlob(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.NoError(t, fs.Mkdir("my/dir1"))
	assert.NoError(t, fs.WriteFile(model.CreateFile("my/dir2/file.txt", "foo\n")))

	matches, err := fs.Glob(`my/abc/*`)
	assert.NoError(t, err)
	assert.Empty(t, matches)

	matches, err = fs.Glob(`my/*`)
	assert.NoError(t, err)
	assert.Equal(t, []string{`my/dir1`, `my/dir2`}, matches)

	matches, err = fs.Glob(`my/*/*`)
	assert.NoError(t, err)
	assert.Equal(t, []string{`my/dir2/file.txt`}, matches)

	matches, err = fs.Glob(`my/*/*.txt`)
	assert.NoError(t, err)
	assert.Equal(t, []string{`my/dir2/file.txt`}, matches)
}

func (*testCases) TestStat(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.NoError(t, fs.WriteFile(model.CreateFile("my/dir/file.txt", "foo\n")))
	s, err := fs.Stat(`my/dir/file.txt`)
	assert.NoError(t, err)
	assert.False(t, s.IsDir())
}

func (*testCases) TestReadDir(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.NoError(t, fs.Mkdir("my/dir/subdir"))
	assert.NoError(t, fs.WriteFile(model.CreateFile("my/dir/file.txt", "foo\n")))

	items, err := fs.ReadDir(`my/dir`)
	assert.NoError(t, err)
	assert.Len(t, items, 2)
}

func (*testCases) TestExists(t *testing.T, fs model.Filesystem, log *utils.Writer) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(model.CreateFile(filePath, "foo\n")))

	// Assert
	log.Truncate()
	assert.True(t, fs.Exists(filePath))
	assert.False(t, fs.Exists("file-non-exists.txt"))
	assert.Equal(t, "", log.String())
}

func (*testCases) TestIsFile(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.NoError(t, fs.Mkdir("my/dir"))
	assert.NoError(t, fs.WriteFile(model.CreateFile("my/dir/file.txt", "foo\n")))

	// Assert
	assert.True(t, fs.IsFile("my/dir/file.txt"))
	assert.False(t, fs.IsFile("my/dir"))
	assert.False(t, fs.IsFile("file-non-exists.txt"))
}

func (*testCases) TestIsDir(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.NoError(t, fs.Mkdir("my/dir"))
	assert.NoError(t, fs.WriteFile(model.CreateFile("my/dir/file.txt", "foo\n")))

	// Assert
	assert.True(t, fs.IsDir("my/dir"))
	assert.False(t, fs.IsDir("my/dir/file.txt"))
	assert.False(t, fs.IsDir("file-non-exists.txt"))
}

func (*testCases) TestCreate(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	fd, err := fs.Create(`file.txt`)
	assert.NoError(t, err)

	n, err := fd.WriteString("foo\n")
	assert.Equal(t, 4, n)
	assert.NoError(t, err)
	assert.NoError(t, fd.Close())

	file, err := fs.ReadFile(`file.txt`, ``)
	assert.NoError(t, err)
	assert.Equal(t, "foo\n", file.Content)
}

func (*testCases) TestOpen(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.NoError(t, fs.WriteFile(model.CreateFile(`file.txt`, "foo\n")))

	fd, err := fs.Open(`file.txt`)
	assert.NoError(t, err)

	content, err := io.ReadAll(fd)
	assert.NoError(t, err)
	assert.Equal(t, "foo\n", string(content))
	assert.NoError(t, fd.Close())
}

func (*testCases) TestOpenFile(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.NoError(t, fs.WriteFile(model.CreateFile(`file.txt`, "foo\n")))

	fd, err := fs.OpenFile(`file.txt`, os.O_RDONLY, 0600)
	assert.NoError(t, err)

	content, err := io.ReadAll(fd)
	assert.NoError(t, err)
	assert.Equal(t, "foo\n", string(content))
	assert.NoError(t, fd.Close())
}

func (*testCases) TestMkdir(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.False(t, fs.Exists("my/dir"))
	assert.NoError(t, fs.Mkdir("my/dir"))
	assert.True(t, fs.Exists("my/dir"))
	assert.NoError(t, fs.Mkdir("my/dir"))
	assert.True(t, fs.Exists("my/dir"))
}

func (*testCases) TestCopyFile(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.NoError(t, fs.Mkdir("my/dir1"))
	assert.NoError(t, fs.Mkdir("my/dir2"))
	assert.NoError(t, fs.WriteFile(model.CreateFile("my/dir1/file.txt", "foo\n")))

	assert.True(t, fs.Exists("my/dir1/file.txt"))
	assert.False(t, fs.Exists("my/dir2/file.txt"))

	assert.NoError(t, fs.Copy("my/dir1/file.txt", "my/dir2/file.txt"))

	assert.True(t, fs.Exists("my/dir1/file.txt"))
	assert.True(t, fs.Exists("my/dir2/file.txt"))
}

func (*testCases) TestCopyFileExists(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.NoError(t, fs.Mkdir("my/dir1"))
	assert.NoError(t, fs.Mkdir("my/dir2"))
	assert.NoError(t, fs.WriteFile(model.CreateFile("my/dir1/file.txt", "foo\n")))
	assert.NoError(t, fs.WriteFile(model.CreateFile("my/dir2/file.txt", "bar\n")))

	assert.True(t, fs.Exists("my/dir1/file.txt"))
	assert.True(t, fs.Exists("my/dir2/file.txt"))
	err := fs.Copy("my/dir1/file.txt", "my/dir2/file.txt")
	assert.Error(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), `cannot copy "my/dir1/file.txt" -> "my/dir2/file.txt": destination exists`))
}

func (*testCases) TestCopyForce(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.NoError(t, fs.Mkdir("my/dir1"))
	assert.NoError(t, fs.Mkdir("my/dir2"))
	assert.NoError(t, fs.WriteFile(model.CreateFile("my/dir1/file.txt", "foo\n")))
	assert.NoError(t, fs.WriteFile(model.CreateFile("my/dir2/file.txt", "bar\n")))

	assert.True(t, fs.Exists("my/dir1/file.txt"))
	assert.True(t, fs.Exists("my/dir2/file.txt"))
	assert.NoError(t, fs.CopyForce("my/dir1/file.txt", "my/dir2/file.txt"))

	file, err := fs.ReadFile("my/dir2/file.txt", "")
	assert.NoError(t, err)
	assert.Equal(t, "foo\n", file.Content)
	assert.True(t, fs.Exists("my/dir1/file.txt"))
	assert.True(t, fs.Exists("my/dir2/file.txt"))
}

func (*testCases) TestCopyDir(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.NoError(t, fs.Mkdir("my/dir1"))
	assert.NoError(t, fs.WriteFile(model.CreateFile("my/dir1/file.txt", "foo\n")))

	assert.True(t, fs.Exists("my/dir1/file.txt"))
	assert.False(t, fs.Exists("my/dir2/file.txt"))

	assert.NoError(t, fs.Copy("my/dir1", "my/dir2"))

	assert.True(t, fs.Exists("my/dir1/file.txt"))
	assert.True(t, fs.Exists("my/dir2/file.txt"))
}

func (*testCases) TestCopyDirExists(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.NoError(t, fs.Mkdir("my/dir1"))
	assert.NoError(t, fs.WriteFile(model.CreateFile("my/dir1/file.txt", "foo\n")))
	assert.NoError(t, fs.Mkdir("my/dir2"))

	assert.True(t, fs.Exists("my/dir1/file.txt"))
	assert.True(t, fs.Exists("my/dir2"))
	err := fs.Copy("my/dir1", "my/dir2")
	assert.True(t, strings.HasPrefix(err.Error(), `cannot copy "my/dir1" -> "my/dir2": destination exists`))
}

func (*testCases) TestMoveFile(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.NoError(t, fs.Mkdir("my/dir1"))
	assert.NoError(t, fs.Mkdir("my/dir2"))
	assert.NoError(t, fs.WriteFile(model.CreateFile("my/dir1/file.txt", "foo\n")))

	assert.True(t, fs.Exists("my/dir1/file.txt"))
	assert.False(t, fs.Exists("my/dir2/file.txt"))

	assert.NoError(t, fs.Move("my/dir1/file.txt", "my/dir2/file.txt"))

	assert.False(t, fs.Exists("my/dir1/file.txt"))
	assert.True(t, fs.Exists("my/dir2/file.txt"))
}

func (*testCases) TestMoveFileExists(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.NoError(t, fs.Mkdir("my/dir1"))
	assert.NoError(t, fs.Mkdir("my/dir2"))
	assert.NoError(t, fs.WriteFile(model.CreateFile("my/dir1/file.txt", "foo\n")))
	assert.NoError(t, fs.WriteFile(model.CreateFile("my/dir2/file.txt", "bar\n")))

	assert.True(t, fs.Exists("my/dir1/file.txt"))
	assert.True(t, fs.Exists("my/dir2/file.txt"))
	err := fs.Move("my/dir1/file.txt", "my/dir2/file.txt")
	assert.Error(t, err)
	assert.Equal(t, `cannot move "my/{dir1/file.txt -> dir2/file.txt}": destination exists`, err.Error())
}

func (*testCases) TestMoveForce(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.NoError(t, fs.Mkdir("my/dir1"))
	assert.NoError(t, fs.Mkdir("my/dir2"))
	assert.NoError(t, fs.WriteFile(model.CreateFile("my/dir1/file.txt", "foo\n")))
	assert.NoError(t, fs.WriteFile(model.CreateFile("my/dir2/file.txt", "bar\n")))

	assert.True(t, fs.Exists("my/dir1/file.txt"))
	assert.True(t, fs.Exists("my/dir2/file.txt"))
	assert.NoError(t, fs.MoveForce("my/dir1/file.txt", "my/dir2/file.txt"))

	file, err := fs.ReadFile("my/dir2/file.txt", "")
	assert.NoError(t, err)
	assert.Equal(t, "foo\n", file.Content)
	assert.False(t, fs.Exists("my/dir1/file.txt"))
	assert.True(t, fs.Exists("my/dir2/file.txt"))
}

func (*testCases) TestMoveDir(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.NoError(t, fs.Mkdir("my/dir1"))
	assert.NoError(t, fs.WriteFile(model.CreateFile("my/dir1/file.txt", "foo\n")))

	assert.True(t, fs.Exists("my/dir1/file.txt"))
	assert.False(t, fs.Exists("my/dir2/file.txt"))

	assert.NoError(t, fs.Move("my/dir1", "my/dir2"))

	assert.False(t, fs.Exists("my/dir1/file.txt"))
	assert.True(t, fs.Exists("my/dir2/file.txt"))
}

func (*testCases) TestMoveDirExists(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.NoError(t, fs.Mkdir("my/dir1"))
	assert.NoError(t, fs.WriteFile(model.CreateFile("my/dir1/file.txt", "foo\n")))
	assert.NoError(t, fs.Mkdir("my/dir2"))

	assert.True(t, fs.Exists("my/dir1/file.txt"))
	assert.True(t, fs.Exists("my/dir2"))
	err := fs.Move("my/dir1", "my/dir2")
	assert.Equal(t, `cannot move "my/{dir1 -> dir2}": destination exists`, err.Error())
}

func (*testCases) TestRemoveFile(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.NoError(t, fs.Mkdir("my/dir1"))
	assert.NoError(t, fs.WriteFile(model.CreateFile("my/dir1/file.txt", "foo\n")))

	assert.True(t, fs.Exists("my/dir1/file.txt"))
	assert.NoError(t, fs.Remove("my/dir1/file.txt"))
	assert.False(t, fs.Exists("my/dir1/file.txt"))
}

func (*testCases) TestRemoveDir(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.NoError(t, fs.Mkdir("my/dir1"))
	assert.NoError(t, fs.WriteFile(model.CreateFile("my/dir1/file.txt", "foo\n")))

	assert.True(t, fs.Exists("my/dir1"))
	assert.NoError(t, fs.Remove("my/dir1"))
	assert.False(t, fs.Exists("my/dir1"))
}

func (*testCases) TestRemoveNotExist(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	assert.NoError(t, fs.Remove("my/dir1/file.txt"))
}

func (*testCases) TestReadFile(t *testing.T, fs model.Filesystem, log *utils.Writer) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(model.CreateFile(filePath, "foo\n")))

	// Read
	log.Truncate()
	file, err := fs.ReadFile(filePath, "")
	assert.NoError(t, err)
	assert.NotNil(t, file)
	assert.Equal(t, "foo\n", file.Content)
	assert.Equal(t, `DEBUG  Loaded "file.txt"`, strings.TrimSpace(log.String()))
}

func (*testCases) TestReadFileNotFound(t *testing.T, fs model.Filesystem, log *utils.Writer) {
	filePath := "file.txt"
	file, err := fs.ReadFile(filePath, "")
	assert.Error(t, err)
	assert.Nil(t, file)
	assert.True(t, strings.HasPrefix(err.Error(), `missing file "file.txt"`))
	assert.Equal(t, "", log.String())
}

func (*testCases) TestWriteFile(t *testing.T, fs model.Filesystem, log *utils.Writer) {
	filePath := "file.txt"

	// Write
	assert.NoError(t, fs.WriteFile(model.CreateFile(filePath, "foo\n")))
	assert.Equal(t, `DEBUG  Saved "file.txt"`, strings.TrimSpace(log.String()))

	// Read
	file, err := fs.ReadFile(filePath, "")
	assert.NoError(t, err)
	assert.NotNil(t, file)
	assert.Equal(t, "foo\n", file.Content)
}

func (*testCases) TestWriteFileDirNotFound(t *testing.T, fs model.Filesystem, log *utils.Writer) {
	filePath := "my/dir/file.txt"

	// Write
	assert.NoError(t, fs.WriteFile(model.CreateFile(filePath, "foo\n")))
	expectedLogs := `
DEBUG  Created directory "my/dir"
DEBUG  Saved "my/dir/file.txt"
`
	assert.Equal(t, strings.TrimSpace(expectedLogs), strings.TrimSpace(log.String()))

	// Read - dir is auto created
	file, err := fs.ReadFile(filePath, "")
	assert.NoError(t, err)
	assert.NotNil(t, file)
	assert.Equal(t, "foo\n", file.Content)
}

func (*testCases) TestWriteJsonFile(t *testing.T, fs model.Filesystem, log *utils.Writer) {
	filePath := "file.json"

	// Write
	data := utils.NewOrderedMap()
	data.Set(`foo`, `bar`)
	assert.NoError(t, fs.WriteJsonFile(model.CreateJsonFile(filePath, data)))
	assert.Equal(t, `DEBUG  Saved "file.json"`, strings.TrimSpace(log.String()))

	// Read
	file, err := fs.ReadFile(filePath, "")
	assert.NoError(t, err)
	assert.NotNil(t, file)
	assert.Equal(t, "{\n  \"foo\": \"bar\"\n}\n", file.Content)
}

func (*testCases) TestCreateOrUpdateFile(t *testing.T, fs model.Filesystem, log *utils.Writer) {
	filePath := "file.txt"

	// Create empty file
	updated, err := fs.CreateOrUpdateFile(filePath, "", []model.FileLine{})
	assert.False(t, updated)
	assert.NoError(t, err)
	assert.True(t, fs.Exists(filePath))
	file, err := fs.ReadFile(filePath, "")
	assert.NoError(t, err)
	assert.Equal(t, "", file.Content)

	// Add some lines
	updated, err = fs.CreateOrUpdateFile(filePath, "", []model.FileLine{
		{Line: "foo"},
		{Line: "bar\n"},
		{Line: "BAZ1=123\n", Regexp: "^BAZ1="},
		{Line: "BAZ2=456\n", Regexp: "^BAZ2=.*$"},
	})
	assert.NoError(t, err)
	assert.True(t, updated)
	assert.True(t, fs.Exists(filePath))
	file, err = fs.ReadFile(filePath, "")
	assert.NoError(t, err)
	assert.Equal(t, "foo\nbar\nBAZ1=123\nBAZ2=456\n", file.Content)

	// Update some lines
	updated, err = fs.CreateOrUpdateFile(filePath, "", []model.FileLine{
		{Line: "BAZ1=new123\n", Regexp: "^BAZ1="},
		{Line: "BAZ2=new456\n", Regexp: "^BAZ2=.*$"},
	})
	assert.True(t, updated)
	assert.NoError(t, err)
	assert.True(t, fs.Exists(filePath))
	file, err = fs.ReadFile(filePath, "")
	assert.NoError(t, err)
	assert.Equal(t, "foo\nbar\nBAZ1=new123\nBAZ2=new456\n", file.Content)
}

func (*testCases) TestReadJsonFile(t *testing.T, fs model.Filesystem, log *utils.Writer) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(model.CreateFile(filePath, `{"foo": "bar"}`)))

	// Read
	log.Truncate()
	file, err := fs.ReadJsonFile(filePath, "")
	assert.NoError(t, err)
	assert.NotNil(t, file)
	assert.Equal(t, `{"foo":"bar"}`, json.MustEncodeString(file.Content, false))
	assert.Equal(t, `DEBUG  Loaded "file.txt"`, strings.TrimSpace(log.String()))
}

func (*testCases) TestReadJsonFileTo(t *testing.T, fs model.Filesystem, log *utils.Writer) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(model.CreateFile(filePath, `{"foo": "bar"}`)))

	// Read
	log.Truncate()
	target := &myStruct{}
	err := fs.ReadJsonFileTo(filePath, "", target)
	assert.NoError(t, err)
	assert.Equal(t, `bar`, target.FooField)
	assert.Equal(t, `DEBUG  Loaded "file.txt"`, strings.TrimSpace(log.String()))
}

func (*testCases) TestReadJsonFileToInvalid(t *testing.T, fs model.Filesystem, log *utils.Writer) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(model.CreateFile(filePath, `{"foo":`)))

	// Read
	log.Truncate()
	target := &myStruct{}
	err := fs.ReadJsonFileTo(filePath, "", target)
	assert.Error(t, err)
	expectedError := `
file "file.txt" is invalid:
	- unexpected end of JSON input, offset: 7
`
	assert.Equal(t, strings.TrimSpace(expectedError), err.Error())
}

func (*testCases) TestReadJsonFileInvalid(t *testing.T, fs model.Filesystem, _ *utils.Writer) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(model.CreateFile(filePath, `{"foo":`)))

	// Read
	file, err := fs.ReadJsonFile(filePath, "")
	assert.Error(t, err)
	assert.Nil(t, file)
	expectedError := `
file "file.txt" is invalid:
	- unexpected end of JSON input, offset: 7
`
	assert.Equal(t, strings.TrimSpace(expectedError), err.Error())
}

func (*testCases) TestReadJsonFieldsTo(t *testing.T, fs model.Filesystem, log *utils.Writer) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(model.CreateFile(filePath, `{"field1": "foo", "field2": "bar"}`)))

	// Read
	log.Truncate()
	target := &myStruct{}
	file, err := fs.ReadJsonFieldsTo(filePath, "", target, `mytag:field`)
	assert.NoError(t, err)
	assert.NotNil(t, file)
	assert.Equal(t, `{"field1":"foo","field2":"bar"}`, json.MustEncodeString(file.Content, false))
	assert.Equal(t, `foo`, target.Field1)
	assert.Equal(t, `bar`, target.Field2)
	assert.Equal(t, `DEBUG  Loaded "file.txt"`, strings.TrimSpace(log.String()))
}

func (*testCases) TestReadJsonMapTo(t *testing.T, fs model.Filesystem, log *utils.Writer) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(model.CreateFile(filePath, `{"field1": "foo", "field2": "bar"}`)))

	// Read
	log.Truncate()
	target := &myStruct{}
	file, err := fs.ReadJsonMapTo(filePath, "", target, `mytag:map`)
	assert.NoError(t, err)
	assert.NotNil(t, file)
	assert.Equal(t, `{"field1":"foo","field2":"bar"}`, json.MustEncodeString(file.Content, false))
	assert.Equal(t, `{"field1":"foo","field2":"bar"}`, json.MustEncodeString(target.Map, false))
	assert.Equal(t, `DEBUG  Loaded "file.txt"`, strings.TrimSpace(log.String()))
}

func (*testCases) TestReadFileContentTo(t *testing.T, fs model.Filesystem, log *utils.Writer) {
	// Create file
	filePath := "file.txt"
	assert.NoError(t, fs.WriteFile(model.CreateFile(filePath, `{"field1": "foo", "field2": "bar"}`)))

	// Read
	log.Truncate()
	target := &myStruct{}
	file, err := fs.ReadFileContentTo(filePath, "", target, `mytag:content`)
	assert.NoError(t, err)
	assert.NotNil(t, file)
	assert.Equal(t, `{"field1": "foo", "field2": "bar"}`, file.Content)
	assert.Equal(t, `{"field1": "foo", "field2": "bar"}`, target.Content)
	assert.Equal(t, `DEBUG  Loaded "file.txt"`, strings.TrimSpace(log.String()))
}

func TestRel(t *testing.T) {
	assert.Equal(t, "abc/file.txt", Rel(`foo/bar`, `foo/bar/abc/file.txt`))
}

func TestJoin(t *testing.T) {
	assert.Equal(t, `foo/bar/abc/file.txt`, Join("foo/bar/abc", "file.txt"))
}

func TestSplit(t *testing.T) {
	dir, file := Split(`foo/bar/abc/file.txt`)
	assert.Equal(t, "foo/bar/abc/", dir)
	assert.Equal(t, "file.txt", file)
}

func TestDir(t *testing.T) {
	assert.Equal(t, "foo/bar/abc", Dir(`foo/bar/abc/file.txt`))
}

func TestBase(t *testing.T) {
	assert.Equal(t, "file.txt", Base(`foo/bar/abc/file.txt`))
}

func TestMatch(t *testing.T) {
	m, err := Match(`foo/*/*/*`, `foo/bar/abc/file.txt`)
	assert.NoError(t, err)
	assert.True(t, m)

	m, err = Match(`abc/**`, `foo/bar/abc/file.txt`)
	assert.NoError(t, err)
	assert.False(t, m)
}
