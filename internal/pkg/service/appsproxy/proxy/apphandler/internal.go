package apphandler

import (
	"net/http"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
	svcErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
)

// isKnownAuthPath lists every internal path an auth handler answers: the provider selector,
// the basic auth form and the oauth2-proxy routes. A new auth endpoint must be added here.
func isKnownAuthPath(path string) bool {
	switch path {
	case config.InternalPrefix + "/selection",
		config.InternalPrefix + "/sign_out",
		config.InternalPrefix + "/callback",
		config.InternalPrefix + "/form",
		config.InternalPrefix + "/sign_in",
		config.InternalPrefix + "/start",
		config.InternalPrefix + "/userinfo",
		config.InternalPrefix + "/auth":
		return true
	}
	return strings.HasPrefix(path, config.InternalPrefix+"/static/")
}

func isInternalPath(path string) bool {
	return path == config.InternalPrefix || strings.HasPrefix(path, config.InternalPrefix+"/")
}

func internalPathNotFound(path string) error {
	return svcErrors.NewResourceNotFoundError("route for", path, "application")
}

// rejectInternalPaths stops every internal path before the upstream, whichever auth handler let it through.
func rejectInternalPaths(next chain.Handler) chain.Handler {
	return chain.HandlerFunc(func(w http.ResponseWriter, req *http.Request) error {
		if isInternalPath(req.URL.Path) {
			return internalPathNotFound(req.URL.Path)
		}
		return next.ServeHTTPOrError(w, req)
	})
}
