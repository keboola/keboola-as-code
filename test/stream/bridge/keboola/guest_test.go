package keboola_test

import (
	"context"
	"crypto/rand"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	utilsproject "github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/require"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/netutils"
)

// To see details run: TEST_VERBOSE=true go test ./test/stream/bridge/... -v

func TestGuestUserWorkflow(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	secretKey := make([]byte, 32)
	_, err := rand.Read(secretKey)
	require.NoError(t, err)

	modifyConfig := func(cfg *config.Config) {
		cfg.Encryption.Native.SecretKey = string(secretKey)
		apiPort := netutils.FreePortForTest(t)
		cfg.API.Listen = "0.0.0.0:" + strconv.FormatInt(int64(apiPort), 10)
		u, err := url.Parse("http://localhost:" + strconv.FormatInt(int64(apiPort), 10))
		require.NoError(t, err)
		cfg.API.PublicURL = u
	}
	ts := setup(
		t,
		ctx,
		modifyConfig,
		utilsproject.WithIsGuest(),
	)
	ts.setupSourceThroughAPI(t, ctx, http.StatusForbidden)
	defer ts.teardown(t, ctx)
	recreateStreamAPI(t, &ts, ctx, modifyConfig)
	ts.setupSourceThroughAPI(t, ctx, http.StatusOK)

	recreateStreamAPI(t, &ts, ctx, modifyConfig, utilsproject.WithIsGuest())
	ts.setupSinkThroughAPI(t, ctx, http.StatusForbidden)
}

func recreateStreamAPI(t *testing.T, ts *testState, ctx context.Context, modifyConfig func(cfg *config.Config), options ...utilsproject.Option) {
	t.Helper()

	// Kill existing API as we are changing project
	ts.apiScp.Process().Shutdown(ctx, errors.New("bye bye"))
	ts.apiScp.Process().WaitForShutdown()

	// Setup new project without guest user to setup source
	ts.setupProject(t, options...)
	ts.apiScp, ts.apiMock = dependencies.NewMockedAPIScopeWithConfig(
		t,
		ctx,
		func(c *config.Config) {
			c.NodeID = "api"
			modifyConfig(c)
		},
		commonDeps.WithEtcdConfig(ts.etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
		commonDeps.WithTestProject(ts.project),
	)
	require.NoError(t, api.Start(ctx, ts.apiScp, ts.apiMock.TestConfig()))
}
