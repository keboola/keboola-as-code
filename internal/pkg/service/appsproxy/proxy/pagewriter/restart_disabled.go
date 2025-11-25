package pagewriter

import (
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
)

type RestartDisabledPageData struct {
	App AppData
}

func (pw *Writer) WriteRestartDisabledPage(w http.ResponseWriter, req *http.Request, app api.AppConfig) {
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate;")
	w.Header().Set("pragma", "no-cache")
	pw.writePage(w, req, "restart_disabled.gohtml", http.StatusBadRequest, RestartDisabledPageData{
		App: NewAppData(&app),
	})
}
