package pagewriter

import "net/http"

type PreviewLandingPageData struct {
	Nonce string
}

type PreviewSessionRequiredPageData struct {
	Nonce         string
	ParentOrigins []string
	MessageType   string
}

func (pw *Writer) WritePreviewLandingPage(w http.ResponseWriter, req *http.Request, nonce string) {
	pw.writePage(w, req, "preview_landing.gohtml", http.StatusOK, PreviewLandingPageData{Nonce: nonce})
}

func (pw *Writer) WritePreviewSessionRequiredPage(w http.ResponseWriter, req *http.Request, data PreviewSessionRequiredPageData) {
	pw.writePage(w, req, "preview_session_required.gohtml", http.StatusOK, data)
}
