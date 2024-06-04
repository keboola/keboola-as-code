package wakeup_test

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/wakeup"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

func TestManager_Wakeup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clk := clock.NewMock()
	d, mock := dependencies.NewMockedServiceScope(t, config.New(), commonDeps.WithClock(clk))

	appID := api.AppID("app")

	transport := mock.MockedHTTPTransport()
	transport.RegisterResponder(
		http.MethodPatch,
		fmt.Sprintf("%s/apps/%s", mock.TestConfig().SandboxesAPI.URL, appID),
		httpmock.NewStringResponder(http.StatusOK, ""),
	)

	manager := d.WakeupManager()

	// The first request is send to the API
	err := manager.Wakeup(ctx, appID)
	require.NoError(t, err)
	assert.Equal(t, 1, transport.GetTotalCallCount())

	// Request is skipped, the Interval was not exceeded
	clk.Add(time.Millisecond)
	err = manager.Wakeup(ctx, appID)
	require.NoError(t, err)
	assert.Equal(t, 1, transport.GetTotalCallCount())

	// Exceed the Interval
	clk.Add(wakeup.Interval)
	err = manager.Wakeup(ctx, appID)
	require.NoError(t, err)
	assert.Equal(t, 2, transport.GetTotalCallCount())
}

func TestManager_Wakeup_Race(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clk := clock.NewMock()
	d, mock := dependencies.NewMockedServiceScope(t, config.New(), commonDeps.WithClock(clk))

	appID := api.AppID("app")

	transport := mock.MockedHTTPTransport()
	transport.RegisterResponder(
		http.MethodPatch,
		fmt.Sprintf("%s/apps/%s", mock.TestConfig().SandboxesAPI.URL, appID),
		httpmock.NewStringResponder(http.StatusOK, ""),
	)

	manager := d.WakeupManager()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wg := sync.WaitGroup{}
	counter := atomic.NewInt64(0)
	// Load configuration 10x in parallel
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := manager.Wakeup(ctx, appID)
			require.NoError(t, err)

			counter.Add(1)
		}()
	}

	// Wait for all requests
	wg.Wait()

	// Check total goroutines/requests count
	assert.Equal(t, int64(10), counter.Load())
	assert.Equal(t, 1, transport.GetTotalCallCount())
}
