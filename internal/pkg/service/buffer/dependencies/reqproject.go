package dependencies

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/token"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

// projectRequestScope implements ProjectRequestScope interface.
type projectRequestScope struct {
	PublicRequestScope
	dependencies.ProjectScope
	logger       log.Logger
	tokenManager *token.Manager
	tableManager *table.Manager
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
	d.logger = pubReqScp.Logger()
	d.tokenManager = token.NewManager(d)
	d.tableManager = table.NewManager(d.KeboolaProjectAPI())
	return d
}

func (v *projectRequestScope) Logger() log.Logger {
	return v.logger
}

func (v *projectRequestScope) TokenManager() *token.Manager {
	return v.tokenManager
}

func (v *projectRequestScope) TableManager() *table.Manager {
	return v.tableManager
}
