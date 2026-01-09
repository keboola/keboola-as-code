package service

import (
	"context"
	"strings"

	"github.com/dimfeld/httptreemux/v5"
	"goa.design/goa/v3/security"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (s *service) APIKeyAuth(ctx context.Context, tokenStr string, scheme *security.APIKeyScheme) (context.Context, error) {
	if scheme.Name != "storage-api-token" {
		panic(errors.Errorf("unexpected security scheme: %#v", scheme))
	}

	pubReqScp := ctx.Value(dependencies.PublicRequestScopeCtxKey).(dependencies.PublicRequestScope)

	// Setup project request scope, it includes authentication
	prjReqScp, err := dependencies.NewProjectRequestScope(ctx, pubReqScp, tokenStr)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, dependencies.ProjectRequestScopeCtxKey, prjReqScp)
	ctx = context.WithValue(ctx, dependencies.KeboolaProjectAPICtxKey, prjReqScp.KeboolaProjectAPI())

	// Setup branch, source and sink request scopes, if applicable
	ctx, err = s.setupResourceScopes(ctx, prjReqScp)
	return ctx, err
}

// setupResourceScopes sets up branch, source, and sink request scopes based on route parameters.
func (s *service) setupResourceScopes(ctx context.Context, prjReqScp dependencies.ProjectRequestScope) (context.Context, error) {
	routerData := httptreemux.ContextData(ctx)
	if routerData == nil {
		return ctx, nil
	}

	route := routerData.Route()
	branch := strings.Contains(route, ":branchId")
	source := branch && strings.Contains(route, ":sourceId")
	sink := source && strings.Contains(route, ":sinkId")

	if !branch {
		return ctx, nil
	}

	// Setup branch scope
	branchID := key.BranchIDOrDefault(routerData.Params()["branchId"])
	branchReqScp, err := dependencies.NewBranchRequestScope(ctx, prjReqScp, branchID)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, dependencies.BranchRequestScopeCtxKey, branchReqScp)

	if !source {
		return ctx, nil
	}

	// Setup source scope
	sourceID := key.SourceID(routerData.Params()["sourceId"])
	sourceReqScp := dependencies.NewSourceRequestScope(branchReqScp, sourceID)
	ctx = context.WithValue(ctx, dependencies.SourceRequestScopeCtxKey, sourceReqScp)

	if !sink {
		return ctx, nil
	}

	// Setup sink scope
	sinkID := key.SinkID(routerData.Params()["sinkId"])
	sinkReqScp := dependencies.NewSinkRequestScope(sourceReqScp, sinkID)
	ctx = context.WithValue(ctx, dependencies.SinkRequestScopeCtxKey, sinkReqScp)

	return ctx, nil
}
