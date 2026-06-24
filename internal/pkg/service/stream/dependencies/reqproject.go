package dependencies

import (
	"context"
	"net/http"
	"strconv"
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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

	prjScp, err := resolveProjectScope(ctx, pubReqScp, tokenStr)
	if err != nil {
		return nil, err
	}

	return newProjectRequestScope(pubReqScp, prjScp), nil
}

// resolveProjectScope builds the project scope from the request token. A
// programmatic token (kbc_at_*/kbc_pat_*) is exchanged for the project's Storage
// token via Connection's auth-bridge (project named by the X-KBC-ProjectId
// header); a legacy Storage token takes the normal verify path. The master-token
// requirement is preserved either way.
func resolveProjectScope(ctx context.Context, pubReqScp PublicRequestScope, tokenStr string) (dependencies.ProjectScope, error) {
	if !dependencies.IsProgrammaticToken(tokenStr) {
		return dependencies.NewProjectDeps(ctx, pubReqScp, tokenStr)
	}

	projectID, err := strconv.Atoi(middleware.ProjectIDFromHeader(ctx))
	if err != nil || projectID <= 0 {
		return nil, svcerrors.WrapWithStatusCode(
			errors.Errorf("programmatic token request missing valid %s header", middleware.ProjectIDHeader),
			http.StatusBadRequest,
		)
	}

	return dependencies.ExchangeProgrammaticToken(ctx, pubReqScp, pubReqScp.KubernetesTokenPath(), tokenStr, projectID)
}

func NewMockedProjectRequestScope(tb testing.TB, ctx context.Context, opts ...dependencies.MockedOption) (ProjectRequestScope, Mocked) {
	tb.Helper()
	pubReqScp, mock := NewMockedPublicRequestScope(tb, ctx, opts...)
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
