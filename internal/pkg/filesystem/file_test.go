package filesystem

import (
	"testing"

	"github.com/google/go-jsonnet/ast"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestCreateFile(t *testing.T) {
	t.Parallel()
	f := NewFile(`path`, `content`).SetDescription(`desc`)
	assert.Equal(t, `path`, f.Path)
	assert.Equal(t, `desc`, f.Desc)
	assert.Equal(t, `content`, f.Content)
}

func TestCreateJsonFile(t *testing.T) {
	t.Parallel()
	m := orderedmap.New()
	f := NewJsonFile(`path`, m).SetDescription(`desc`)
	assert.Equal(t, `path`, f.Path)
	assert.Equal(t, `desc`, f.Desc)
	assert.Equal(t, m, f.Content)
}

func TestJsonFile_ToFile(t *testing.T) {
	t.Parallel()
	m := orderedmap.New()
	m.Set(`foo`, `bar`)
	f, err := NewJsonFile(`path`, m).SetDescription(`desc`).ToFile()
	assert.NoError(t, err)
	assert.Equal(t, `path`, f.Path)
	assert.Equal(t, `desc`, f.Desc)
	assert.Equal(t, "{\n  \"foo\": \"bar\"\n}\n", f.Content)
}

func TestCreateJsonNetFile(t *testing.T) {
	t.Parallel()
	astNode := &ast.Object{}
	f := NewJsonNetFile(`path`, astNode).SetDescription(`desc`)
	assert.Equal(t, `path`, f.Path)
	assert.Equal(t, `desc`, f.Desc)
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
	file, err := jsonNetFile.ToFile()
	assert.NoError(t, err)
	assert.Equal(t, `path`, file.Path)
	assert.Equal(t, `desc`, file.Desc)
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
	jsonNetFile := NewJsonNetFile(`path`, astNode).SetDescription(`desc`)
	jsonFile, err := jsonNetFile.ToJsonFile()
	assert.NoError(t, err)
	assert.Equal(t, `path`, jsonFile.Path)
	assert.Equal(t, `desc`, jsonFile.Desc)
	assert.Equal(t, orderedmap.FromPairs([]orderedmap.Pair{{Key: "foo", Value: "bar"}}), jsonFile.Content)
}
