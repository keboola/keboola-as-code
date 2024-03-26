package file

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
)

func (r *Repository) openFileOnSinkActivation() {
	r.plugins.Collection().OnSinkActivation(func(ctx *plugin.SaveContext, old, updated *definition.Sink) {

	})
}
