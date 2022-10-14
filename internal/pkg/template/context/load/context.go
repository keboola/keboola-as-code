// Package load represents the process of template loading from the filesystem.
package load

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet/fsimporter"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template/jsonnet/function"
)

// Context represents the process of template loading from the filesystem.
type Context struct {
	_context
	components *model.ComponentsMap
	jsonNetCtx *jsonnet.Context
}

type _context context.Context

func NewContext(ctx context.Context, objectsRoot filesystem.Fs, components *model.ComponentsMap) *Context {
	c := &Context{
		_context:   ctx,
		components: components,
		jsonNetCtx: jsonnet.NewContext().WithCtx(ctx).WithImporter(fsimporter.New(objectsRoot)),
	}

	// Register JsonNet functions
	c.registerJsonNetFunctions()

	return c
}

func (c *Context) JsonNetContext() *jsonnet.Context {
	return c.jsonNetCtx
}

func (c *Context) registerJsonNetFunctions() {
	c.jsonNetCtx.NativeFunctionWithAlias(function.ComponentIsAvailable(c.components))
	c.jsonNetCtx.NativeFunctionWithAlias(function.SnowflakeWriterComponentId(c.components))
}
