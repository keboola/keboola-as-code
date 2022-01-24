package filesystem

import (
	"testing"

	"github.com/google/go-jsonnet/ast"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestCreateFile(t *testing.T) {
	t.Parallel()
	f := NewRawFile(`path`, `content`)
	f.SetDescription(`desc`)
	assert.Equal(t, `path`, f.Path())
	assert.Equal(t, `desc`, f.Description())
	assert.Equal(t, `content`, f.Content)
}

func TestCreateJsonFile(t *testing.T) {
	t.Parallel()
	m := orderedmap.New()
	f := NewJsonFile(`path`, m)
	f.SetDescription(`desc`)
	assert.Equal(t, `path`, f.Path())
	assert.Equal(t, `desc`, f.Description())
	assert.Equal(t, m, f.Content)
}

func TestJsonFile_ToFile(t *testing.T) {
	t.Parallel()
	m := orderedmap.New()
	m.Set(`foo`, `bar`)
	f, err := NewJsonFile(`path`, m).SetDescription(`desc`).ToRawFile()
	assert.NoError(t, err)
	assert.Equal(t, `path`, f.Path())
	assert.Equal(t, `desc`, f.Description())
	assert.Equal(t, "{\n  \"foo\": \"bar\"\n}\n", f.Content)
}

func TestCreateJsonNetFile(t *testing.T) {
	t.Parallel()
	astNode := &ast.Object{}
	f := NewJsonNetFile(`path`, astNode)
	f.SetDescription(`desc`)
	assert.Equal(t, `path`, f.Path())
	assert.Equal(t, `desc`, f.Description())
	assert.Equal(t, astNode, f.Content)
}

func TestJsonNetFile_ToFile(t *testing.T) {
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
