package pagewriter

import (
	"html/template"
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
)

type LoginPageData struct {
	App        AppData
	FormAction string
	CsrfField  template.HTML
	Error      error
}

func (pw *Writer) WriteLoginPage(w http.ResponseWriter, req *http.Request, app *api.AppConfig, formAction string, csrfField template.HTML, err error) {
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate;")
	w.Header().Set("pragma", "no-cache")
	pw.writePage(w, req, "login.gohtml", http.StatusOK, &LoginPageData{App: NewAppData(app), FormAction: formAction, CsrfField: csrfField, Error: err})
}
