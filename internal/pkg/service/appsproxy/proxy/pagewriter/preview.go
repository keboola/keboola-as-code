package pagewriter

import "net/http"

type PreviewLandingPageData struct {
	Nonce string
}

func (pw *Writer) WritePreviewLandingPage(w http.ResponseWriter, req *http.Request, nonce string) {
	pw.writePage(w, req, "preview_landing.gohtml", http.StatusOK, PreviewLandingPageData{Nonce: nonce})
}
