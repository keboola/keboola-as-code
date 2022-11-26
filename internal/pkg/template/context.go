package template

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/replacevalues"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// NewContext disables validation of the "required_in_project" rule for templates.
func NewContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, validator.DisableRequiredInProjectKey, true)
}

type Context interface {
	context.Context
	RemoteObjectsFilter() model.ObjectsFilter
	LocalObjectsFilter() model.ObjectsFilter
	JSONNETContext() *jsonnet.Context
	Replacements() (*replacevalues.Values, error)
}
