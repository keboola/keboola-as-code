package dependencies

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/config"
)

func NewMockedAPIScope(t *testing.T, cfg config.Config, opts ...dependencies.MockedOption) (APIScope, dependencies.Mocked) {
	t.Helper()

	opts = append(opts, dependencies.WithEnabledEtcdClient())
	mocked := dependencies.NewMocked(t, opts...)

	var err error
	cfg.StorageAPIHost = mocked.StorageAPIHost()
	cfg.PublicAddress, err = url.Parse("https://templates.keboola.local")
	require.NoError(t, err)
	cfg.Etcd = mocked.TestEtcdConfig()

	// Prepare test repository with templates, instead of default repositories, to prevent loading of all production templates.
	if reflect.DeepEqual(cfg.Repositories, config.DefaultRepositories()) {
		tmpDir := t.TempDir()
		assert.NoError(t, aferofs.CopyFs2Fs(nil, filesystem.Join("git_test", "repository"), nil, tmpDir))
		assert.NoError(t, os.Rename(filepath.Join(tmpDir, ".gittest"), filepath.Join(tmpDir, ".git"))) // nolint:forbidigo
		cfg.Repositories = []model.TemplateRepository{{
			Type: model.RepositoryTypeGit, Name: "keboola", URL: fmt.Sprintf("file://%s", tmpDir), Ref: "main",
		}}
	}

	// Validate config
	require.NoError(t, cfg.Validate())

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
