package filesystem

import (
	"testing"

	"github.com/google/go-jsonnet/ast"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
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
	f, err := NewRawFile(`path`, `{"foo": "bar"}`).ToJsonFile()
	assert.NoError(t, err)
	assert.Equal(t, `path`, f.Path())
	assert.Equal(t, orderedmap.FromPairs([]orderedmap.Pair{{Key: "foo", Value: "bar"}}), f.Content)
}

func TestRawFile_ToJsonNetFile(t *testing.T) {
	t.Parallel()
	f, err := NewRawFile(`path`, `{foo:"bar"}`).ToJsonNetFile()
	assert.NoError(t, err)
	assert.Equal(t, `path`, f.Path())
	assert.Equal(t, "{\n  \"foo\": \"bar\"\n}\n", jsonnet.MustEvaluateAst(f.Content))
}

func TestNewJsonFile(t *testing.T) {
	t.Parallel()
	m := orderedmap.New()
	f := NewJsonFile(`path`, m)
	f.SetDescription(`desc`)
	assert.Equal(t, `path`, f.Path())
	assert.Equal(t, `desc`, f.Description())
	assert.Equal(t, m, f.Content)
}

func TestJsonFile_ToRawFile(t *testing.T) {
	t.Parallel()
	m := orderedmap.New()
	m.Set(`foo`, `bar`)
	f, err := NewJsonFile(`path`, m).SetDescription(`desc`).ToRawFile()
	assert.NoError(t, err)
	assert.Equal(t, `path`, f.Path())
	assert.Equal(t, `desc`, f.Description())
	assert.Equal(t, "{\n  \"foo\": \"bar\"\n}\n", f.Content)
}

func TestJsonFile_ToJsonNetFile(t *testing.T) {
	t.Parallel()
	m := orderedmap.New()
	m.Set(`foo`, `bar`)
	f, err := NewJsonFile(`path.json`, m).ToJsonNetFile()
	assert.NoError(t, err)
	assert.Equal(t, `path.jsonnet`, f.Path())
	assert.Equal(t, "{\n  foo: \"bar\",\n}\n", jsonnet.MustFormatAst(f.Content))
}

func TestNewJsonNetFile(t *testing.T) {
	t.Parallel()
	astNode := &ast.Object{}
	f := NewJsonNetFile(`path`, astNode)
	f.SetDescription(`desc`)
	assert.Equal(t, `path`, f.Path())
	assert.Equal(t, `desc`, f.Description())
	assert.Equal(t, astNode, f.Content)
}

func TestJsonNetFile_ToRawFile(t *testing.T) {
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
	jsonNetFile := NewJsonNetFile(`path`, astNode).SetDescription(`desc`)
	file, err := jsonNetFile.ToRawFile()
	assert.NoError(t, err)
	assert.Equal(t, `path`, file.Path())
	assert.Equal(t, `desc`, file.Description())
	assert.Equal(t, "{ foo: \"bar\" }\n", file.Content)
}

func TestJsonNetFile_ToJsonFile(t *testing.T) {
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
	jsonNetFile := NewJsonNetFile(`path`, astNode)
	jsonNetFile.SetDescription(`desc`)
	jsonFile, err := jsonNetFile.ToJsonFile()
	assert.NoError(t, err)
	assert.Equal(t, `path`, jsonFile.Path())
	assert.Equal(t, `desc`, jsonFile.Description())
	assert.Equal(t, orderedmap.FromPairs([]orderedmap.Pair{{Key: "foo", Value: "bar"}}), jsonFile.Content)
}

func TestJsonNetFile_ToRawJsonFile(t *testing.T) {
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
	jsonNetFile := NewJsonNetFile(`path`, astNode)
	jsonNetFile.SetDescription(`desc`)
	rawJsonFile, err := jsonNetFile.ToJsonRawFile()
	assert.NoError(t, err)
	assert.Equal(t, `path`, rawJsonFile.Path())
	assert.Equal(t, `desc`, rawJsonFile.Description())
	assert.Equal(t, "{\n  \"foo\": \"bar\"\n}\n", rawJsonFile.Content)
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
