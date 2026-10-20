package apptoken

import (
	"context"
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
)

const EmailVerifiedHeader = "X-Kbc-User-Email-Verified"

type IdentityProvider struct {
	URL string
}

type identityProviderCtxKey struct{}

// WithIdentityProvider marks the request as admitted by an OAuth provider.
// Only such a request gets a token.
func WithIdentityProvider(next chain.Handler, idp IdentityProvider) chain.Handler {
	return chain.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) error {
		ctx := context.WithValue(req.Context(), identityProviderCtxKey{}, idp)
		return next.ServeHTTPOrError(rw, req.WithContext(ctx))
	})
}

func identityProviderFromContext(ctx context.Context) (IdentityProvider, bool) {
	idp, ok := ctx.Value(identityProviderCtxKey{}).(IdentityProvider)
	return idp, ok
}
