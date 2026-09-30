package apphandler

import (
	"context"
	"net"
	"net/http"

	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview/session"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
	svcErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
)

const landingNonceLength = 24

// previewOrigin never uses h.baseURL for a Sandbox: there it names the parent App.
func (h *appHandler) previewOrigin(ctx context.Context) (string, bool) {
	if !h.workload.IsSandbox() {
		origin, err := preview.NormalizeOrigin(h.baseURL.Scheme + "://" + h.baseURL.Host)
		return origin, err == nil
	}
	info, ok := h.manager.upstreamManager.AppInfo(ctx, h.workload)
	if !ok || info.PublicHost == "" {
		return "", false
	}
	publicURL := h.manager.config.API.PublicURL
	host := info.PublicHost
	if port := publicURL.Port(); port != "" {
		host = net.JoinHostPort(host, port)
	}
	origin, err := preview.NormalizeOrigin(publicURL.Scheme + "://" + host)
	return origin, err == nil
}

func (h *appHandler) servePreviewEndpoint(w http.ResponseWriter, req *http.Request) error {
	if h.manager.preview == nil {
		return svcErrors.NewResourceNotFoundError("route for", req.URL.Path, "application")
	}
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Cache-Control", "no-store")
	switch req.Method {
	case http.MethodGet, http.MethodHead:
		nonce := idgenerator.Random(landingNonceLength)
		w.Header().Set("Content-Security-Policy", preview.LandingCSP(nonce, h.manager.preview.FrameAncestors()))
		h.manager.pageWriter.WritePreviewLandingPage(w, req, nonce)
	case http.MethodPost:
		h.redeemPreviewLink(w, req)
	default:
		w.Header().Set("Allow", "GET, HEAD, POST")
		h.writePreviewError(w, req, http.StatusMethodNotAllowed, "Method not allowed.")
	}
	return nil
}

func (h *appHandler) previewSessionValid(w http.ResponseWriter, req *http.Request, raw string) bool {
	if raw == "" || h.manager.preview == nil {
		return false
	}
	ctx := req.Context()
	if !h.isDevMode(ctx) {
		return false
	}
	origin, ok := h.previewOrigin(ctx)
	if !ok {
		return false
	}
	_, refresh, valid := h.manager.preview.Sessions().Check(raw, origin)
	if !valid {
		return false
	}
	if refresh != nil {
		http.SetCookie(w, refresh)
	}
	return true
}

func (h *appHandler) clearPreviewSession(w http.ResponseWriter, raw string) {
	if raw == "" || h.manager.preview == nil {
		return
	}
	http.SetCookie(w, session.ClearCookie())
}

func (h *appHandler) redeemPreviewLink(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	if !preview.IsSameOriginRedeem(req) {
		h.manager.logger.Info(ctx, "preview: redeem rejected, not submitted from the landing page")
		h.writePreviewError(w, req, http.StatusForbidden, preview.CrossSiteRedeemMessage)
		return
	}
	req.Body = http.MaxBytesReader(w, req.Body, preview.MaxRedeemBodySize)
	if err := req.ParseForm(); err != nil {
		h.manager.logger.Info(ctx, "preview: redeem rejected, unreadable form")
		h.writePreviewError(w, req, http.StatusUnauthorized, preview.InvalidLinkMessage)
		return
	}
	origin, ok := h.previewOrigin(ctx)
	if !ok || !h.isDevMode(ctx) {
		h.manager.logger.Info(ctx, "preview: redeem rejected, host is not a dev-mode workload")
		h.writePreviewError(w, req, http.StatusUnauthorized, preview.InvalidLinkMessage)
		return
	}
	claims, err := h.manager.preview.Links().Verify(ctx, req.PostForm.Get(preview.TokenFormField), origin)
	if err != nil {
		h.manager.logger.With(attribute.String("preview.kid", log.Sanitize(claims.Kid))).Infof(ctx, "preview: redeem rejected: %s", err)
		h.writePreviewError(w, req, http.StatusUnauthorized, preview.InvalidLinkMessage)
		return
	}
	cookie, err := h.manager.preview.Sessions().Issue(origin, claims.ID)
	if err != nil {
		h.manager.pageWriter.WriteError(w, req, &h.app, err)
		return
	}
	h.manager.logger.With(
		attribute.String("preview.linkJti", log.Sanitize(claims.ID)),
		attribute.String("preview.kid", log.Sanitize(claims.Kid)),
	).Info(ctx, "preview: link redeemed")
	http.SetCookie(w, cookie)
	w.Header().Set("Location", "/")
	w.WriteHeader(http.StatusSeeOther)
}

func (h *appHandler) writePreviewError(w http.ResponseWriter, req *http.Request, status int, message string) {
	h.manager.pageWriter.WriteErrorPage(w, req, &h.app, status, message, pagewriter.ExceptionIDPrefix+svcErrors.GenerateExceptionID())
}
