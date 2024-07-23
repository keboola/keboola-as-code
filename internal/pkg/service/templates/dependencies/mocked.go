package dependencies

import (
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/config"
)

func NewMockedAPIScope(t *testing.T, cfg config.Config, opts ...dependencies.MockedOption) (APIScope, dependencies.Mocked) {
	t.Helper()

	opts = append(opts, dependencies.WithEnabledEtcdClient(), dependencies.WithEnabledTasks("test-node"))
	mocked := dependencies.NewMocked(t, opts...)

	var err error
	cfg.StorageAPIHost = mocked.StorageAPIHost()
	cfg.API.PublicURL, err = url.Parse("https://templates.keboola.local")
	require.NoError(t, err)
	cfg.Etcd = mocked.TestEtcdConfig()

	// Prepare test repository with templates, instead of default repositories, to prevent loading of all production templates.
	if reflect.DeepEqual(cfg.Repositories, config.DefaultRepositories()) {
		tmpDir := t.TempDir()
		_, filename, _, _ := runtime.Caller(0)
		srcFs, err := aferofs.NewLocalFs(path.Dir(filename))
		require.NoError(t, err)
		require.NoError(t, aferofs.CopyFs2Fs(srcFs, filesystem.Join("git_test", "repository"), nil, tmpDir))
		require.NoError(t, os.Rename(filepath.Join(tmpDir, ".gittest"), filepath.Join(tmpDir, ".git"))) // nolint:forbidigo
		cfg.Repositories = []model.TemplateRepository{{
			Type: model.RepositoryTypeGit, Name: "keboola", URL: fmt.Sprintf("file://%s", tmpDir), Ref: "main",
		}}
	}

	if cfg.NodeID == "" {
		cfg.NodeID = "my-node"
	}

	// Validate configuration
	require.NoError(t, configmap.ValidateAndNormalize(&cfg))

	apiScp, err := newAPIScope(mocked.TestContext(), mocked, cfg)
	require.NoError(t, err)

	mocked.DebugLogger().Truncate()
	return apiScp, mocked
}

func NewMockedPublicRequestScope(t *testing.T, cfg config.Config, opts ...dependencies.MockedOption) (PublicRequestScope, dependencies.Mocked) {
	t.Helper()
	apiScp, mock := NewMockedAPIScope(t, cfg, opts...)
	pubReqScp := newPublicRequestScope(apiScp, mock)
	mock.DebugLogger().Truncate()
	return pubReqScp, mock
}

func NewMockedProjectRequestScope(t *testing.T, cfg config.Config, opts ...dependencies.MockedOption) (ProjectRequestScope, dependencies.Mocked) {
	t.Helper()
	pubReqScp, mock := NewMockedPublicRequestScope(t, cfg, opts...)
	prjReqSp := newProjectRequestScope(pubReqScp, mock)
	mock.DebugLogger().Truncate()
	return prjReqSp, mock
}
