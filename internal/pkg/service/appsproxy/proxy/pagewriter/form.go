package pagewriter

import "net/http"

type FormPageData struct {
	App AppData
}

func (pw *Writer) WriteFormPage(w http.ResponseWriter, req *http.Request, status int) {
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate;")
	w.Header().Set("pragma", "no-cache")
	pw.writePage(w, req, "form.gohtml", status, nil)
}
