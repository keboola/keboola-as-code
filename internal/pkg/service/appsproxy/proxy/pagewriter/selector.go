package pagewriter

import (
	"net/http"
)

type SelectorPageData struct {
	Providers []ProviderData
}

type ProviderData struct {
	Name string
	URL  string
}

func (pw *Writer) WriteSelectorPage(w http.ResponseWriter, req *http.Request, status int, data *SelectorPageData) {
	// Render page
	pw.writePage(w, req, "selector.gohtml", status, data)
}
