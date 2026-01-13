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
	if err == nil {
		ctx = context.WithValue(ctx, dependencies.ProjectRequestScopeCtxKey, prjReqScp)
		ctx = context.WithValue(ctx, dependencies.KeboolaProjectAPICtxKey, prjReqScp.KeboolaProjectAPI())
	} else {
		return nil, err
	}

	routerData := httptreemux.ContextData(ctx)
	if routerData == nil {
		return ctx, err
	}

	// Setup branch, source and sink request scopes
	branch := strings.Contains(routerData.Route(), ":branchId")
	source := branch && strings.Contains(routerData.Route(), ":sourceId")
	sink := source && strings.Contains(routerData.Route(), ":sinkId")

	var branchReqScp dependencies.BranchRequestScope
	if branch {
		branchID := key.BranchIDOrDefault(routerData.Params()["branchId"])
		if branchReqScp, err = dependencies.NewBranchRequestScope(ctx, prjReqScp, branchID); err == nil {
			ctx = context.WithValue(ctx, dependencies.BranchRequestScopeCtxKey, branchReqScp)
		} else {
			return nil, err
		}
	}

	var sourceReqScp dependencies.SourceRequestScope
	if source {
		sourceID := key.SourceID(routerData.Params()["sourceId"])
		sourceReqScp = dependencies.NewSourceRequestScope(branchReqScp, sourceID)
		ctx = context.WithValue(ctx, dependencies.SourceRequestScopeCtxKey, sourceReqScp)
	}

	if sink {
		sinkID := key.SinkID(routerData.Params()["sinkId"])
		sinkReqScp := dependencies.NewSinkRequestScope(sourceReqScp, sinkID)
		ctx = context.WithValue(ctx, dependencies.SinkRequestScopeCtxKey, sinkReqScp)
	}

	return ctx, err
}
