// Package upload contains the implementation of the "kbc project remote file upload" command.
package upload

import (
	"context"

	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

type dependencies interface {
	KeboolaProjectAPI() *keboola.API
	Logger() log.Logger
	Tracer() trace.Tracer
}

type Options struct {
	Input string
	Name  string
	Tags  []string
}

func Run(_ context.Context, _ Options, _ dependencies) (err error) {
	return nil
}
