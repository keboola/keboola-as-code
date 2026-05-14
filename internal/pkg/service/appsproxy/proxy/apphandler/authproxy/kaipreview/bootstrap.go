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
}

func NewBootstrapHandler(allowedIDEOrigins []string) *BootstrapHandler {
	bs, _ := json.Marshal(allowedIDEOrigins) // []string round-trip never errors for []string
	return &BootstrapHandler{
		allowedIDEOrigins: allowedIDEOrigins,
		originsJSON:       template.JS(bs),
	}
}

func (h *BootstrapHandler) ServeHTTPOrError(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
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
