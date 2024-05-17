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
	if scheme.Name == "storage-api-token" {
		pubReqScp := ctx.Value(dependencies.PublicRequestScopeCtxKey).(dependencies.PublicRequestScope)

		// Setup project request scope, it includes authentication
		prjReqScp, err := dependencies.NewProjectRequestScope(ctx, pubReqScp, tokenStr)
		if err == nil {
			ctx = context.WithValue(ctx, dependencies.ProjectRequestScopeCtxKey, prjReqScp)
			ctx = context.WithValue(ctx, dependencies.KeboolaProjectAPICtxKey, prjReqScp.KeboolaProjectAPI())
		} else {
			return nil, err
		}

		// Setup branch, source and sink request scopes, if applicable
		if routerData := httptreemux.ContextData(ctx); routerData != nil {
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
		}

		return ctx, err
	}

	panic(errors.Errorf("unexpected security scheme: %#v", scheme))
}
