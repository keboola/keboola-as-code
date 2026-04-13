package dependencies

import (
	"context"
	"net/url"
	"path"
	"reflect"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distlock"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/config"
)

func NewMockedAPIScope(tb testing.TB, ctx context.Context, cfg config.Config, opts ...dependencies.MockedOption) (APIScope, dependencies.Mocked) {
	tb.Helper()

	opts = append(opts, dependencies.WithEnabledEtcdClient())
	mock := dependencies.NewMocked(tb, ctx, opts...)

	var err error
	cfg.StorageAPIHost = mock.StorageAPIHost()
	cfg.API.PublicURL, err = url.Parse("https://templates.keboola.local")
	require.NoError(tb, err)
	cfg.Etcd = mock.TestEtcdConfig()

	// Prepare test repository with templates, instead of default repositories, to prevent loading of all production templates.
	if reflect.DeepEqual(cfg.Repositories, config.DefaultRepositories()) {
		_, filename, _, _ := runtime.Caller(0)
		cfg.Repositories = []model.TemplateRepository{{
			Type: model.RepositoryTypeDir, Name: "keboola", URL: filesystem.Join(path.Dir(filename), "git_test", "repository"),
		}}
	}

	if cfg.NodeID == "" {
		cfg.NodeID = "my-node"
	}

	// Validate configuration
	require.NoError(tb, configmap.ValidateAndNormalize(&cfg))

	p := &parentScopes{
		BaseScope:         mock,
		PublicScope:       mock,
		EtcdClientScope:   mock,
		DistributionScope: dependencies.NewDistributionScope(cfg.NodeID, distribution.NewConfig(), mock),
	}

	p.DistributedLockScope, err = dependencies.NewDistributedLockScope(ctx, distlock.NewConfig(), mock)
	require.NoError(tb, err)

	p.TaskScope, err = dependencies.NewTaskScope(ctx, cfg.NodeID, exceptionIDPrefix, mock, mock, p.DistributionScope, cfg.API.Task)
	require.NoError(tb, err)

	apiScp, err := newAPIScope(ctx, p, cfg)
	require.NoError(tb, err)

	mock.DebugLogger().Truncate()
	return apiScp, mock
}

func NewMockedPublicRequestScope(tb testing.TB, ctx context.Context, cfg config.Config, opts ...dependencies.MockedOption) (PublicRequestScope, dependencies.Mocked) {
	tb.Helper()
	apiScp, mock := NewMockedAPIScope(tb, ctx, cfg, opts...)
	pubReqScp := newPublicRequestScope(apiScp, mock)
	mock.DebugLogger().Truncate()
	return pubReqScp, mock
}

func NewMockedProjectRequestScope(tb testing.TB, ctx context.Context, cfg config.Config, opts ...dependencies.MockedOption) (ProjectRequestScope, dependencies.Mocked) {
	tb.Helper()
	pubReqScp, mock := NewMockedPublicRequestScope(tb, ctx, cfg, opts...)
	prjReqSp := newProjectRequestScope(pubReqScp, mock)
	mock.DebugLogger().Truncate()
	return prjReqSp, mock
}
