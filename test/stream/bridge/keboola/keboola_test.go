package keboola_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	stagingConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/config"
	targetConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/config"
)

type withProcess interface {
	Process() *servicectx.Process
}

func TestKeboolaBridgeWorkflow(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	configFn := func(cfg *config.Config) {
		cfg.Storage.Level.Staging.Upload.Trigger = stagingConfig.UploadTrigger{
			Count:    10,
			Size:     1000 * datasize.MB,
			Interval: duration.From(30 * time.Minute),
		}
		cfg.Storage.Level.Target.Import.Trigger = targetConfig.ImportTrigger{
			Count:       20,
			Size:        1000 * datasize.MB,
			Interval:    duration.From(30 * time.Minute),
			SlicesCount: 100,
			Expiration:  duration.From(30 * time.Minute),
		}
	}

	ts := setup(t, ctx, configFn)
	defer ts.teardown(t, ctx)

	// TODO: choose source scope
	for i := range 100 {
		req, err := http.NewRequest(http.MethodPost, ts.sourceURL1, strings.NewReader(fmt.Sprintf("foo%d", i)))
		require.NoError(t, err)
		resp, err := ts.httpClient.Do(req)
		if assert.NoError(t, err) {
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.NoError(t, resp.Body.Close())
		}
	}

	time.Sleep(10 * time.Second)
	ts.logger.AssertJSONMessages(t, "")
}

func (ts *testState) logSection(t *testing.T, section string) {
	t.Logf("\n\n\n----------------------------\n%s\n----------------------------\n\n\n\n", section)
}

func formatHTTPSourceURL(t *testing.T, baseURL string, entity definition.Source) string {
	u, err := url.Parse(baseURL)
	require.NoError(t, err)
	return u.
		JoinPath("stream", entity.ProjectID.String(), entity.SourceID.String(), entity.HTTP.Secret).
		String()
}
