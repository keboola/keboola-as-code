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
		} else {
			return nil, err
		}

		// Setup branch request scope, if applicable
		if routerData := httptreemux.ContextData(ctx); routerData != nil {
			if strings.Contains(routerData.Route(), ":branchId") {
				branchID := key.BranchIDOrDefault(routerData.Params()["branchId"])
				if branchReqScp, err := dependencies.NewBranchRequestScope(ctx, prjReqScp, branchID); err == nil {
					ctx = context.WithValue(ctx, dependencies.BranchRequestScopeCtxKey, branchReqScp)
				} else {
					return nil, err
				}
			}
		}

		return ctx, err
	}

	panic(errors.Errorf("unexpected security scheme: %#v", scheme))
}
