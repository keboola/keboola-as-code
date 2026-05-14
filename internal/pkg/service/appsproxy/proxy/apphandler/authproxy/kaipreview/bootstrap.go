package kaipreview

import (
	"embed"
	"encoding/json"
	"html/template"
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

//go:embed template/bootstrap.gohtml
var bootstrapFS embed.FS

var bootstrapTmpl = template.Must(template.ParseFS(bootstrapFS, "template/bootstrap.gohtml"))

type BootstrapHandler struct {
	allowedIDEOrigins []string
	originsJSON       template.JS
	devMode           DevModeChecker
	appID             string
}

func NewBootstrapHandler(allowedIDEOrigins []string, devMode DevModeChecker, appID string) *BootstrapHandler {
	bs, _ := json.Marshal(allowedIDEOrigins) // []string round-trip never errors for []string
	return &BootstrapHandler{
		allowedIDEOrigins: allowedIDEOrigins,
		originsJSON:       template.JS(bs),
		devMode:           devMode,
		appID:             appID,
	}
}

func (h *BootstrapHandler) ServeHTTPOrError(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return nil
	}

	// Dev-mode gate: pretend the endpoint doesn't exist on non-dev apps.
	if !h.devMode.IsDevMode(r.Context(), h.appID) {
		http.NotFound(w, r)
		return nil
	}

	WriteFrameAncestorsCSP(w, h.allowedIDEOrigins)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")

	data := struct {
		AllowedIDEOriginsJSON template.JS
	}{
		AllowedIDEOriginsJSON: h.originsJSON,
	}
	if err := bootstrapTmpl.Execute(w, data); err != nil {
		return errors.Errorf("kai-preview: render bootstrap shim: %w", err)
	}
	return nil
}
