package model

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestCreateFile(t *testing.T) {
	f := CreateFile(`path`, `desc`, `content`)
	assert.Equal(t, FilePath(`path`), f.Path)
	assert.Equal(t, `desc`, f.Desc)
	assert.Equal(t, `content`, f.Content)
}

func TestCreateJsonFile(t *testing.T) {
	m := utils.NewOrderedMap()
	f := CreateJsonFile(`path`, `desc`, m)
	assert.Equal(t, FilePath(`path`), f.Path)
	assert.Equal(t, `desc`, f.Desc)
	assert.Equal(t, m, f.Content)
}

func TestJsonFile_ToFile(t *testing.T) {
	m := utils.NewOrderedMap()
	m.Set(`foo`, `bar`)
	f, err := CreateJsonFile(`path`, `desc`, m).ToFile()
	assert.NoError(t, err)
	assert.Equal(t, FilePath(`path`), f.Path)
	assert.Equal(t, `desc`, f.Desc)
	assert.Equal(t, "{\n  \"foo\": \"bar\"\n}\n", f.Content)
}
