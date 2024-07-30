package testnode

import (
	"fmt"
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/netutils"
)

func StartAPINode(tb testing.TB, logger log.DebugLogger, etcdCfg etcdclient.Config, modifyConfig func(cfg *config.Config), opts ...commonDeps.MockedOption) (dependencies.APIScope, dependencies.Mocked) {
	tb.Helper()

	opts = append(opts, commonDeps.WithDebugLogger(logger), commonDeps.WithEtcdConfig(etcdCfg))
	return dependencies.NewMockedAPIScopeWithConfig(
		tb,
		func(cfg *config.Config) {
			if modifyConfig != nil {
				modifyConfig(cfg)
			}
			cfg.NodeID = "api"
			cfg.Hostname = "localhost"
			cfg.API.Listen = fmt.Sprintf("0.0.0.0:%d", netutils.FreePortForTest(tb))
		},
		opts...,
	)
}
