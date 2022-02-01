package template

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/replacevalues"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func baseContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, validator.DisableRequiredInProjectKey, true)
}

type _context context.Context

type Context interface {
	context.Context
	RemoteObjectsFilter() model.ObjectsFilter
	LocalObjectsFilter() model.ObjectsFilter
	JsonNetContext() *jsonnet.Context
	Replacements() (*replacevalues.Values, error)
}
