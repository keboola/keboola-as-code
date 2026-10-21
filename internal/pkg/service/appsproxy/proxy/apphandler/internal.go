package apphandler

import (
	"net/http"
	"path"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview/session"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/selector"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
	svcErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
)

// serveInternal routes the internal paths. The auth paths are the provider selector,
// the basic auth form and the oauth2-proxy routes. A new auth endpoint must be added here.
func (h *appHandler) serveInternal(w http.ResponseWriter, req *http.Request) error {
	path := req.URL.Path
	if path == preview.Path {
		return h.servePreviewEndpoint(w, req)
	}

	previewCookie := session.TakeCookie(req)
	if path == selector.SignOutPath {
		h.clearPreviewSession(w, previewCookie)
	}

	if h.allAuthHandlers == nil {
		return internalPathNotFound(path)
	}

	switch path {
	case selector.SignOutPath:
		// A sign-out ends the session explicitly. Internal paths never reach
		// the upstream, so this cannot be handled by the sessions middleware.
		h.manager.sessionsManager.SignOut(w, req, h.app)
		return h.allAuthHandlers.ServeHTTPOrError(w, req)
	case config.InternalPrefix + "/selection",
		config.InternalPrefix + "/callback",
		config.InternalPrefix + "/form",
		config.InternalPrefix + "/sign_in",
		config.InternalPrefix + "/start",
		config.InternalPrefix + "/userinfo",
		config.InternalPrefix + "/auth":
		return h.allAuthHandlers.ServeHTTPOrError(w, req)
	}

	if strings.HasPrefix(path, config.InternalPrefix+"/static/") {
		return h.allAuthHandlers.ServeHTTPOrError(w, req)
	}

	return internalPathNotFound(path)
}

// isInternalPath cleans the path first: an encoded "/" or ".." survives the ServeMux cleaning,
// and an app that decodes and cleans its path would serve it as an internal path.
func isInternalPath(p string) bool {
	p = path.Clean(p)
	return p == config.InternalPrefix || strings.HasPrefix(p, config.InternalPrefix+"/")
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
