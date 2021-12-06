package filesystem

import (
	"testing"

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
