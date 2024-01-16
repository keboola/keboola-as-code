package dependencies

import (
	"context"

	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/repository"
)

// serviceScope implements ServiceScope interface.
type definitionScope struct {
	ServiceScope
	definitionRepository *definitionRepo.Repository
}

func (v *definitionScope) DefinitionRepository() *definitionRepo.Repository {
	return v.definitionRepository
}

func NewDefinitionScope(ctx context.Context, svcScope ServiceScope) (v DefinitionScope) {
	ctx, span := svcScope.Telemetry().Tracer().Start(ctx, "keboola.go.buffer.dependencies.NewDefinitionScope")
	defer span.End(nil)

	return newDefinitionScope(svcScope)
}

func newDefinitionScope(svcScope ServiceScope) (v DefinitionScope) {
	d := &definitionScope{}

	d.ServiceScope = svcScope

	d.definitionRepository = definitionRepo.New(d)

	return d
}
