package pagewriter_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
)

type writerDeps struct{}

func (writerDeps) Clock() clockwork.Clock { return clockwork.NewRealClock() }
func (writerDeps) Logger() log.Logger     { return log.NewNopLogger() }

func TestWritePreviewLandingPage(t *testing.T) {
	t.Parallel()
	pw, err := pagewriter.New(writerDeps{})
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://my-app-123.hub.keboola.local/_proxy/preview", nil)
	pw.WritePreviewLandingPage(rec, req, &api.AppConfig{ID: "123", ProjectID: "456"}, "N0nce")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, `<script nonce="N0nce">`)
	assert.Contains(t, body, `action="/_proxy/preview"`)
	assert.Contains(t, body, `name="token"`)
	assert.Contains(t, body, "history.replaceState")
	assert.Contains(t, body, `<meta name="referrer" content="no-referrer">`)
	assert.NotContains(t, body, "<script src")
	assert.NotContains(t, body, `<link rel="stylesheet"`)
	assert.NotContains(t, body, ` style="`)
}
