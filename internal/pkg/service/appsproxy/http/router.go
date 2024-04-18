package http

import (
	"embed"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/appconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Router struct {
	logger            log.Logger
	telemetry         telemetry.Telemetry
	config            config.Config
	clock             clock.Clock
	loader            *appconfig.Loader
	selectionTemplate *template.Template
	exceptionIDPrefix string
	wg                sync.WaitGroup
}

const providerCookie = "_oauth2_provider"

const selectionPagePath = "/_proxy/selection"

//go:embed template/*
var templates embed.FS

func NewRouter(d dependencies.ServiceScope, exceptionIDPrefix string) (*Router, error) {
	html, err := templates.ReadFile("template/selection.html.tmpl")
	if err != nil {
		return nil, errors.PrefixError(err, "selection template file not found")
	}

	tmpl, err := template.New("selection template").Parse(string(html))
	if err != nil {
		return nil, errors.PrefixError(err, "could not parse selection template")
	}

	router := &Router{
		logger:            d.Logger(),
		telemetry:         d.Telemetry(),
		config:            d.Config(),
		clock:             d.Clock(),
		loader:            d.AppConfigLoader(),
		selectionTemplate: tmpl,
		exceptionIDPrefix: exceptionIDPrefix,
		wg:                sync.WaitGroup{},
	}

	return router, nil
}

func (r *Router) Shutdown() {
	r.wg.Wait()
}
