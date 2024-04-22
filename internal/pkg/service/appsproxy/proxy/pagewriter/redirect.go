package pagewriter

import "net/http"

type RedirectPageData struct {
	AppData *AppData
	URL     string
}

func (pw *Writer) WriteRedirectPage(w http.ResponseWriter, req *http.Request, status int, data *RedirectPageData) {
	// Render page
	pw.writePage(w, req, "redirect.gohtml", status, data)
}
