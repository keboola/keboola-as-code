package pagewriter

import (
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
)

type PreviewLandingPageData struct {
	App   AppData
	Nonce string
}

func (pw *Writer) WritePreviewLandingPage(w http.ResponseWriter, req *http.Request, app *api.AppConfig, nonce string) {
	pw.writePage(w, req, "preview_landing.gohtml", http.StatusOK, PreviewLandingPageData{App: NewAppData(app), Nonce: nonce})
}
