package pagewriter

import (
	"bytes"
	"embed"
	"html/template"
	"io/fs"
	"net/http"

	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	robotsPath = "/robots.txt"
	assetsPath = config.InternalPrefix + "/assets/"
)

type Writer struct {
	clock     clockwork.Clock
	logger    log.Logger
	assetsFS  fs.FS
	templates *template.Template
}

type dependencies interface {
	Clock() clockwork.Clock
	Logger() log.Logger
}

//go:embed assets/*
var assetsFS embed.FS

//go:embed template/*
var templatesFS embed.FS

func New(d dependencies) (*Writer, error) {
	assets, err := fs.Sub(assetsFS, "assets")
	if err != nil {
		return nil, err
	}

	templates, err := loadTemplates()
	if err != nil {
		return nil, err
	}

	return &Writer{
		clock:     d.Clock(),
		logger:    d.Logger().WithComponent("page-writer"),
		assetsFS:  assets,
		templates: templates,
	}, nil
}

func (pw *Writer) MountAssets(mux *http.ServeMux) {
	mux.Handle(robotsPath, http.HandlerFunc(pw.WriteRobotsTxt))
	mux.Handle(assetsPath, http.StripPrefix(assetsPath, http.FileServerFS(pw.assetsFS)))
}

func (pw *Writer) writePage(w http.ResponseWriter, req *http.Request, page string, status int, data any) {
	var buf bytes.Buffer

	// Render template
	if err := pw.templates.ExecuteTemplate(&buf, page, data); err != nil {
		err = errors.PrefixErrorf(err, `unable to render page "%s"`, page)
		pw.logger.Error(req.Context(), err.Error())

		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(errors.Format(err, errors.FormatAsSentences())))
		return
	}

	// Send buffer
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(status)
	_, _ = w.Write(buf.Bytes())
}

func loadTemplates() (*template.Template, error) {
	dirFS, err := fs.Sub(templatesFS, "template")
	if err != nil {
		return nil, err
	}
	return template.New("").ParseFS(dirFS, "*.gohtml")
}
