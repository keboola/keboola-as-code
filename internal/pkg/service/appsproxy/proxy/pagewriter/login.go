package pagewriter

import (
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
)

type LoginPageData struct {
	App   AppData
	Error error
}

func (pw *Writer) WriteLoginPage(w http.ResponseWriter, req *http.Request, app *api.AppConfig, err error) {
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate;")
	w.Header().Set("pragma", "no-cache")
	pw.writePage(w, req, "login.gohtml", http.StatusOK, &LoginPageData{App: NewAppData(app), Error: err})
}
