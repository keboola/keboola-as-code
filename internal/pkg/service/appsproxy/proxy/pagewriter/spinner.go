package pagewriter

import (
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
)

type SpinnerPageData struct {
	App AppData
}

func (pw *Writer) WriteSpinnerPage(w http.ResponseWriter, req *http.Request, app api.AppConfig) {
	pw.writePage(w, req, "spinner.gohtml", http.StatusServiceUnavailable, SpinnerPageData{
		App: NewAppData(&app),
	})
}
