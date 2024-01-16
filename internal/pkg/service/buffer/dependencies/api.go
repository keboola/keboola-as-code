package dependencies

import (
	"context"
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/watcher"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type ctxKey string

const (
	PublicRequestScopeCtxKey  = ctxKey("PublicRequestScope")
	ProjectRequestScopeCtxKey = ctxKey("ProjectRequestScope")
	apiUserAgent              = "keboola-buffer-api"
)

// apiSCope implements APIScope interface.
type apiScope struct {
	ServiceScope
	config         config.APIConfig
	statsCollector *statistics.Collector
	watcher        *watcher.APINode
}

func NewAPIScope(
	ctx context.Context,
	cfg config.APIConfig,
	proc *servicectx.Process,
	logger log.Logger,
	tel telemetry.Telemetry,
	stdout io.Writer,
	stderr io.Writer,
) (v APIScope, err error) {
	ctx, span := tel.Tracer().Start(ctx, "keboola.go.buffer.api.dependencies.NewAPIScope")
	defer span.End(&err)
	serviceScp, err := NewServiceScope(ctx, cfg.ServiceConfig, proc, logger, tel, stdout, stderr, apiUserAgent)
	return newAPIScope(cfg, serviceScp)
}

func newAPIScope(cfg config.APIConfig, serviceScp ServiceScope) (v APIScope, err error) {
	d := &apiScope{}

	d.config = cfg

	d.ServiceScope = serviceScp

	d.statsCollector = statistics.NewCollector(d)

	d.watcher, err = watcher.NewAPINode(d)
	if err != nil {
		return nil, err
	}

	return d, nil
}

func (v *apiScope) APIConfig() config.APIConfig {
	return v.config
}

func (v *apiScope) StatsCollector() *statistics.Collector {
	return v.statsCollector
}

func (v *apiScope) WatcherAPINode() *watcher.APINode {
	return v.watcher
}
