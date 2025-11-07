package filesystem

import (
	"testing"

	"github.com/google/go-jsonnet/ast"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
)

func TestNewRawFile(t *testing.T) {
	t.Parallel()
	f := NewRawFile(`path`, `content`)
	f.SetDescription(`desc`)
	assert.Equal(t, `path`, f.Path())
	assert.Equal(t, `desc`, f.Description())
	assert.Equal(t, `content`, f.Content)
}

func TestRawFile_ToJsonFile(t *testing.T) {
	t.Parallel()
	f, err := NewRawFile(`path`, `{"foo": "bar"}`).ToJSONFile()
	require.NoError(t, err)
	assert.Equal(t, `path`, f.Path())
	assert.Equal(t, orderedmap.FromPairs([]orderedmap.Pair{{Key: "foo", Value: "bar"}}), f.Content)
}

func TestRawFile_ToJsonnetFile(t *testing.T) {
	t.Parallel()
	f, err := NewRawFile(`path`, `{foo:"bar"}`).ToJSONNetFile(nil)
	require.NoError(t, err)
	assert.Equal(t, `path`, f.Path())
	assert.JSONEq(t, `{"foo":"bar"}`, jsonnet.MustEvaluateAst(f.Content, nil))
}

func TestNewJsonFile(t *testing.T) {
	t.Parallel()
	m := orderedmap.New()
	f := NewJSONFile(`path`, m)
	f.SetDescription(`desc`)
	assert.Equal(t, `path`, f.Path())
	assert.Equal(t, `desc`, f.Description())
	assert.Equal(t, m, f.Content)
}

func TestJsonFile_ToRawFile(t *testing.T) {
	t.Parallel()
	m := orderedmap.New()
	m.Set(`foo`, `bar`)
	f, err := NewJSONFile(`path`, m).SetDescription(`desc`).ToRawFile()
	require.NoError(t, err)
	assert.Equal(t, `path`, f.Path())
	assert.Equal(t, `desc`, f.Description())
	assert.JSONEq(t, `{"foo":"bar"}`, f.Content)
}

func TestJsonFile_ToJsonnetFile(t *testing.T) {
	t.Parallel()
	m := orderedmap.New()
	m.Set(`foo`, `bar`)
	f, err := NewJSONFile(`path.json`, m).ToJsonnetFile()
	require.NoError(t, err)
	assert.Equal(t, `path.jsonnet`, f.Path())
	jsonnetCode, err := jsonnet.FormatNode(f.Content)
	require.NoError(t, err)
	assert.Equal(t, "{\n  foo: \"bar\",\n}\n", jsonnetCode) //nolint: testifylint
}

func TestNewJsonnetFile(t *testing.T) {
	t.Parallel()
	astNode := &ast.Object{}
	f := NewJsonnetFile(`path`, astNode, nil)
	f.SetDescription(`desc`)
	assert.Equal(t, `path`, f.Path())
	assert.Equal(t, `desc`, f.Description())
	assert.Equal(t, astNode, f.Content)
}

func TestJsonnetFile_ToRawFile(t *testing.T) {
	t.Parallel()
	astNode := &ast.Object{
		Fields: ast.ObjectFields{
			{
				Kind:  ast.ObjectFieldStr,
				Hide:  ast.ObjectFieldInherit,
				Expr1: &ast.LiteralString{Value: "foo"},
				Expr2: &ast.LiteralString{Value: "bar"},
			},
		},
	}
	jsonnetFile := NewJsonnetFile(`path`, astNode, nil).SetDescription(`desc`)
	file, err := jsonnetFile.ToRawFile()
	require.NoError(t, err)
	assert.Equal(t, `path`, file.Path())
	assert.Equal(t, `desc`, file.Description())
	assert.Equal(t, "{ foo: \"bar\" }\n", file.Content) //nolint: testifylint
}

func TestJsonnetFile_ToJsonFile(t *testing.T) {
	t.Parallel()
	astNode := &ast.Object{
		Fields: ast.ObjectFields{
			{
				Kind:  ast.ObjectFieldStr,
				Hide:  ast.ObjectFieldInherit,
				Expr1: &ast.LiteralString{Value: "foo"},
				Expr2: &ast.LiteralString{Value: "bar"},
			},
		},
	}
	jsonnetFile := NewJsonnetFile(`path`, astNode, nil)
	jsonnetFile.SetDescription(`desc`)
	jsonFile, err := jsonnetFile.ToJSONFile()
	require.NoError(t, err)
	assert.Equal(t, `path`, jsonFile.Path())
	assert.Equal(t, `desc`, jsonFile.Description())
	assert.Equal(t, orderedmap.FromPairs([]orderedmap.Pair{{Key: "foo", Value: "bar"}}), jsonFile.Content)
}

func TestJsonnetFile_ToJsonFile_Variables(t *testing.T) {
	t.Parallel()
	ctx := jsonnet.NewContext()
	ctx.ExtVar("myKey", "bar")
	jsonnetFile := NewJsonnetFile(`path`, jsonnet.MustToAst(`{foo: std.extVar("myKey")}`, ""), ctx)
	jsonFile, err := jsonnetFile.ToJSONFile()
	require.NoError(t, err)
	assert.Equal(t, orderedmap.FromPairs([]orderedmap.Pair{{Key: "foo", Value: "bar"}}), jsonFile.Content)
}

func TestJsonnetFile_ToRawJsonFile(t *testing.T) {
	t.Parallel()
	astNode := &ast.Object{
		Fields: ast.ObjectFields{
			{
				Kind:  ast.ObjectFieldStr,
				Hide:  ast.ObjectFieldInherit,
				Expr1: &ast.LiteralString{Value: "foo"},
				Expr2: &ast.LiteralString{Value: "bar"},
			},
		},
	}
	jsonnetFile := NewJsonnetFile(`path`, astNode, nil)
	jsonnetFile.SetDescription(`desc`)
	rawJSONFile, err := jsonnetFile.ToJSONRawFile()
	require.NoError(t, err)
	assert.Equal(t, `path`, rawJSONFile.Path())
	assert.Equal(t, `desc`, rawJSONFile.Description())
	assert.JSONEq(t, `{"foo":"bar"}`, rawJSONFile.Content)
}

func TestFiles(t *testing.T) {
	t.Parallel()
	files := NewFiles()
	assert.Empty(t, files.All())
	assert.Empty(t, files.GetByTag("my-tag"))
	assert.Nil(t, files.GetOneByTag("my-tag"))

	file1 := NewRawFile(`foo1.txt`, `bar1`)
	files.Add(file1)
	assert.Len(t, files.All(), 1)
	assert.Empty(t, files.GetByTag("my-tag"))
	assert.Nil(t, files.GetOneByTag("my-tag"))

	file2 := NewRawFile(`foo2.txt`, `bar2`)
	file2.AddTag("my-tag")
	files.Add(file2)
	assert.Equal(t, []File{file2}, files.GetByTag("my-tag"))
	assert.Equal(t, file2, files.GetOneByTag("my-tag"))

	file3 := NewRawFile(`foo3.txt`, `bar3`)
	file3.AddTag("my-tag")
	files.Add(file3)
	assert.Equal(t, []File{file2, file3}, files.GetByTag("my-tag"))
	assert.PanicsWithError(t, `found multiple files with tag "my-tag": "foo2.txt", "foo3.txt"`, func() {
		assert.Equal(t, file2, files.GetOneByTag("my-tag"))
	})
}
