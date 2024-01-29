package http

import (
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/justinas/alice"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type DataApp struct {
	ID           AppID             `json:"id" validator:"required"`
	Name         string            `json:"name" validator:"required"`
	UpstreamHost string            `json:"upstreamUrl" validator:"required"`
	Provider     *options.Provider `json:"provider,omitempty"`
}

type AppID string

func (v AppID) String() string {
	return string(v)
}

func handlerFor(app DataApp, cfg config.Config) (http.Handler, error) {
	chain := alice.New()
	if app.Provider == nil {
		return publicAppHandler(app, cfg, chain)
	} else {
		return nil, errors.New(`not implemented`)
	}
}

func publicAppHandler(app DataApp, _ config.Config, chain alice.Chain) (http.Handler, error) {
	target, err := url.Parse("http://" + app.UpstreamHost)
	if err != nil {
		return nil, errors.Errorf(`cannot parse upstream url "%s": %w`, app.UpstreamHost, err)
	}
	return chain.Then(httputil.NewSingleHostReverseProxy(target)), nil
}
