package pagewriter

import (
	"io/fs"
	"net/http"
)

func (pw *Writer) WriteRobotsTxt(w http.ResponseWriter, req *http.Request) {
	data, _ := fs.ReadFile(pw.assetsFS, "robots.txt")
	_, _ = w.Write(data)
}
