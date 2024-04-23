package pagewriter

import (
	"net/http"
)

type SelectorPageData struct {
	App       AppData
	Providers []ProviderData
}

type ProviderData struct {
	Name string
	URL  string
}

func (pw *Writer) WriteSelectorPage(w http.ResponseWriter, req *http.Request, status int, data *SelectorPageData) {
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate;")
	w.Header().Set("pragma", "no-cache")
	pw.writePage(w, req, "selector.gohtml", status, data)
}
