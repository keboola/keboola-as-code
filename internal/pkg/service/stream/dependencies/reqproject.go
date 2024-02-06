package dependencies

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

// projectRequestScope implements ProjectRequestScope interface.
type projectRequestScope struct {
	PublicRequestScope
	dependencies.ProjectScope
	logger log.Logger
}

func NewProjectRequestScope(ctx context.Context, pubReqScp PublicRequestScope, tokenStr string) (v ProjectRequestScope, err error) {
	ctx, span := pubReqScp.Telemetry().Tracer().Start(ctx, "keboola.go.buffer.api.dependencies.NewProjectRequestScope")
	defer span.End(&err)

	prjScp, err := dependencies.NewProjectDeps(ctx, pubReqScp, tokenStr)
	if err != nil {
		return nil, err
	}

	return newProjectRequestScope(pubReqScp, prjScp), nil
}

func newProjectRequestScope(pubReqScp PublicRequestScope, prjScp dependencies.ProjectScope) *projectRequestScope {
	d := &projectRequestScope{}

	d.PublicRequestScope = pubReqScp

	d.ProjectScope = prjScp

	loggerPrefix := fmt.Sprintf("[project=%d][token=%s]", d.ProjectID(), d.StorageAPITokenID())
	d.logger = pubReqScp.Logger().AddPrefix(loggerPrefix)

	return d
}

func (v *projectRequestScope) Logger() log.Logger {
	return v.logger
}
