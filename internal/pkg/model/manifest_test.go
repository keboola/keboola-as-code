package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPathsRelatedPaths(t *testing.T) {
	t.Parallel()

	p := Paths{}
	p.SetParentPath(`parent`)
	p.SetRelativePath(`object`)

	p.AddRelatedPath(`parent/object/foo1`)
	p.AddRelatedPath(`parent/object/foo2`)
	p.AddRelatedPath(`parent/object/bar`)
	p.AddRelatedPath(`parent/object/bar/baz`)
	assert.Equal(t, []string{
		`parent/object/foo1`,
		`parent/object/foo2`,
		`parent/object/bar`,
		`parent/object/bar/baz`,
	}, p.GetRelatedPaths())

	p.SetParentPath(`my-parent`)
	p.SetRelativePath(`my-object`)
	assert.Equal(t, []string{
		`my-parent/my-object/foo1`,
		`my-parent/my-object/foo2`,
		`my-parent/my-object/bar`,
		`my-parent/my-object/bar/baz`,
	}, p.GetRelatedPaths())

	p.RenameRelatedPaths(`my-parent/my-object/bar`, `my-parent/my-object/my-bar`)
	assert.Equal(t, []string{
		`my-parent/my-object/foo1`,
		`my-parent/my-object/foo2`,
		`my-parent/my-object/my-bar`,
		`my-parent/my-object/my-bar/baz`,
	}, p.GetRelatedPaths())
}
