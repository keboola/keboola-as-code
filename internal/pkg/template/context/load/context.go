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
	components      *model.ComponentsMap
	jsonnetCtx      *jsonnet.Context
	projectBackends []string
}

type _context context.Context

func NewContext(ctx context.Context, objectsRoot filesystem.Fs, components *model.ComponentsMap, projectBackends []string) *Context {
	c := &Context{
		_context:        ctx,
		components:      components,
		jsonnetCtx:      jsonnet.NewContext().WithCtx(ctx).WithImporter(fsimporter.New(objectsRoot)),
		projectBackends: projectBackends,
	}

	// Register Jsonnet functions
	c.registerJsonnetFunctions()

	return c
}

func (c *Context) JsonnetContext() *jsonnet.Context {
	return c.jsonnetCtx
}

func (c *Context) registerJsonnetFunctions() {
	c.jsonnetCtx.NativeFunctionWithAlias(function.ComponentIsAvailable(c.components))
	c.jsonnetCtx.NativeFunctionWithAlias(function.SnowflakeWriterComponentID(c.components))
	c.jsonnetCtx.NativeFunctionWithAlias(function.HasProjectBackend(c.projectBackends))
}
