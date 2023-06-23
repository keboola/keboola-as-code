package dependencies

import (
	"context"

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
	config  config.APIConfig
	stats   *statistics.CollectorNode
	watcher *watcher.APINode
}

func NewAPIScope(ctx context.Context, cfg config.APIConfig, proc *servicectx.Process, logger log.Logger, tel telemetry.Telemetry) (v APIScope, err error) {
	ctx, span := tel.Tracer().Start(ctx, "keboola.go.buffer.api.dependencies.NewAPIScope")
	defer span.End(&err)
	serviceScp, err := NewServiceScope(ctx, cfg.ServiceConfig, proc, logger, tel, "")
	return newAPIScope(cfg, serviceScp)
}

func newAPIScope(cfg config.APIConfig, serviceScp ServiceScope) (v APIScope, err error) {
	d := &apiScope{}

	d.config = cfg

	d.ServiceScope = serviceScp

	d.stats = statistics.NewCollectorNode(d)

	d.watcher, err = watcher.NewAPINode(d)
	if err != nil {
		return nil, err
	}

	return d, nil
}

func (v *apiScope) APIConfig() config.APIConfig {
	return v.config
}

func (v *apiScope) StatsCollector() *statistics.CollectorNode {
	return v.stats
}

func (v *apiScope) WatcherAPINode() *watcher.APINode {
	return v.watcher
}
