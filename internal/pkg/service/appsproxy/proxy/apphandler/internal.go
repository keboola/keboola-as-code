package apphandler

import (
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview/session"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/selector"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
	svcErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
)

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
		if path == selector.SignOutPath {
			http.Redirect(w, req, "/", http.StatusFound)
			return nil
		}
		return internalPathNotFound(path)
	}

	// A sign-out ends the session explicitly. Internal paths never reach
	// the upstream, so this cannot be handled by the sessions middleware.
	if path == selector.SignOutPath {
		h.manager.sessionsManager.SignOut(w, req, h.app)
	}
	return h.allAuthHandlers.ServeHTTPOrError(w, req)
}

func internalPathNotFound(path string) error {
	return svcErrors.NewResourceNotFoundError("route for", path, "application")
}

// rejectInternalPaths stops every internal path before the upstream, whichever auth handler let it through.
func rejectInternalPaths(next chain.Handler) chain.Handler {
	return chain.HandlerFunc(func(w http.ResponseWriter, req *http.Request) error {
		if config.IsInternalPath(req.URL.Path) {
			return internalPathNotFound(req.URL.Path)
		}
		return next.ServeHTTPOrError(w, req)
	})
}
