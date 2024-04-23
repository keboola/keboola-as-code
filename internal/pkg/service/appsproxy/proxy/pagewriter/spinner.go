package pagewriter

import (
	"net/http"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
)

const (
	spinnerRetryAfter = 5 * time.Second
)

type SpinnerPageData struct {
	App AppData
}

func (pw *Writer) WriteSpinnerPage(w http.ResponseWriter, req *http.Request, app api.AppConfig) {
	w.Header().Set("Retry-After", pw.clock.Now().Add(spinnerRetryAfter).UTC().Format(http.TimeFormat))
	pw.writePage(w, req, "spinner.gohtml", http.StatusServiceUnavailable, SpinnerPageData{
		App: NewAppData(&app),
	})
}
