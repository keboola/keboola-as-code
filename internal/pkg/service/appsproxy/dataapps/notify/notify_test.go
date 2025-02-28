package notify_test

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/notify"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

func TestManager_Notify(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	clk := clockwork.NewFakeClock()
	d, mock := dependencies.NewMockedServiceScope(t, ctx, config.New(), commonDeps.WithClock(clk))

	appID := api.AppID("app")

	transport := mock.MockedHTTPTransport()
	transport.RegisterResponder(
		http.MethodPatch,
		fmt.Sprintf("%s/apps/%s", mock.TestConfig().SandboxesAPI.URL, appID),
		httpmock.NewStringResponder(http.StatusOK, ""),
	)

	manager := d.NotifyManager()

	// The first request is send to the API
	err := manager.Notify(ctx, appID)
	require.NoError(t, err)
	assert.Equal(t, 1, transport.GetTotalCallCount())

	// Request is skipped, the interval was not exceeded
	clk.Advance(time.Millisecond)
	err = manager.Notify(ctx, appID)
	require.NoError(t, err)
	assert.Equal(t, 1, transport.GetTotalCallCount())

	// Exceed the interval
	clk.Advance(notify.Interval)
	err = manager.Notify(ctx, appID)
	require.NoError(t, err)
	assert.Equal(t, 2, transport.GetTotalCallCount())
}

func TestManager_Notify_Race(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	clk := clockwork.NewFakeClock()
	d, mock := dependencies.NewMockedServiceScope(t, ctx, config.New(), commonDeps.WithClock(clk))

	appID := api.AppID("app")

	transport := mock.MockedHTTPTransport()
	transport.RegisterResponder(
		http.MethodPatch,
		fmt.Sprintf("%s/apps/%s", mock.TestConfig().SandboxesAPI.URL, appID),
		httpmock.NewStringResponder(http.StatusOK, ""),
	)

	manager := d.NotifyManager()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	wg := sync.WaitGroup{}
	counter := atomic.NewInt64(0)
	// Load configuration 10x in parallel
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := manager.Notify(ctx, appID)
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
