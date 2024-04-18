package pagewriter

import "net/http"

func (pw *Writer) WriteSpinnerPage(w http.ResponseWriter, req *http.Request) {
	pw.writePage(w, req, "spinner.gohtml", http.StatusServiceUnavailable, nil)
}
