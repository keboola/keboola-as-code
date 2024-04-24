package pagewriter

import "net/http"

type RedirectPageData struct {
	URL string
}

func (pw *Writer) WriteRedirectPage(w http.ResponseWriter, req *http.Request, status int, url string) {
	// Render page
	pw.writePage(w, req, "redirect.gohtml", status, RedirectPageData{URL: url})
}
