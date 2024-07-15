package dependencies

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

// projectRequestScope implements ProjectRequestScope interface.
type projectRequestScope struct {
	PublicRequestScope
	dependencies.ProjectScope
	logger log.Logger
}

func NewProjectRequestScope(ctx context.Context, pubScp PublicRequestScope, tokenStr string) (v ProjectRequestScope, err error) {
	ctx, span := pubScp.Telemetry().Tracer().Start(ctx, "keboola.go.apps.proxy.api.dependencies.NewProjectRequestScope")
	defer span.End(&err)

	return newProjectRequestScope(pubScp), nil
}

func newProjectRequestScope(pubScp PublicRequestScope) *projectRequestScope {
	d := &projectRequestScope{}
	d.PublicRequestScope = pubScp
	d.logger = pubScp.Logger()
	return d
}

func (v *projectRequestScope) Logger() log.Logger {
	return v.logger
}
