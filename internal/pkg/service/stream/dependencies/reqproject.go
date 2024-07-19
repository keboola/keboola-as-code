package dependencies

import (
	"context"
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
)

// projectRequestScope implements ProjectRequestScope interface.
type projectRequestScope struct {
	PublicRequestScope
	dependencies.ProjectScope
	logger log.Logger
}

func NewProjectRequestScope(ctx context.Context, pubReqScp PublicRequestScope, tokenStr string) (v ProjectRequestScope, err error) {
	ctx, span := pubReqScp.Telemetry().Tracer().Start(ctx, "keboola.go.stream.dependencies.NewProjectRequestScope")
	defer span.End(&err)

	prjScp, err := dependencies.NewProjectDeps(ctx, pubReqScp, tokenStr)
	if err != nil {
		return nil, err
	}

	return newProjectRequestScope(pubReqScp, prjScp), nil
}

func NewMockedProjectRequestScope(t *testing.T, opts ...dependencies.MockedOption) (ProjectRequestScope, Mocked) {
	t.Helper()
	pubReqScp, mock := NewMockedPublicRequestScope(t, opts...)
	prjReqScp := newProjectRequestScope(pubReqScp, mock)
	return prjReqScp, mock
}

func newProjectRequestScope(pubReqScp PublicRequestScope, prjScp dependencies.ProjectScope) *projectRequestScope {
	d := &projectRequestScope{}
	d.PublicRequestScope = pubReqScp
	d.ProjectScope = prjScp
	d.logger = pubReqScp.Logger()
	return d
}

func (v *projectRequestScope) Logger() log.Logger {
	return v.logger
}

func (v *projectRequestScope) RequestUser() definition.By {
	return definition.ByFromToken(v.StorageAPIToken())
}
