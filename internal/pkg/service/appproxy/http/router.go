package http

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/mux"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Router struct {
	logger    log.Logger
	telemetry telemetry.Telemetry
	config    config.Config
	handlers  map[AppID]http.Handler
}

func NewRouter(ctx context.Context, d dependencies.ServiceScope, apps []DataApp) *Router {
	router := &Router{
		logger:    d.Logger(),
		telemetry: d.Telemetry(),
		config:    d.Config(),
		handlers:  map[AppID]http.Handler{},
	}

	for _, app := range apps {
		if handler, err := handlerFor(app, router.config); err == nil {
			router.handlers[app.ID] = handler
		} else {
			router.logger.Errorf(ctx, `cannot initialize application "%s": %s`, app.ID, err)
		}
	}

	return router
}

func (r *Router) CreateHandler() http.Handler {
	handler := mux.NewRouter()

	handler.PathPrefix("/").Handler(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		appID, ok := parseAppID(req.URL.Host)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `Unable to parse application ID from the URL.`)
			return
		}

		if handler, found := r.handlers[appID]; found {
			handler.ServeHTTP(w, req)
		} else {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, `Application "%s" not found.`, appID)
		}
	}))

	return handler
}

func parseAppID(host string) (AppID, bool) {
	if strings.Count(host, ".") != 3 {
		return "", false
	}

	idx := strings.IndexByte(host, '.')
	if idx < 0 {
		return "", false
	}

	subdomain := host[:idx]
	idx = strings.LastIndexByte(subdomain, '-')
	if idx < 0 {
		return AppID(subdomain), true
	}

	return AppID(subdomain[idx+1:]), true
}
