package upstream

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRequestLimit(t *testing.T) {
	t.Parallel()

	limit := &requestLimit{slots: make(chan struct{}, 2)}
	assert.True(t, limit.tryAcquire())
	assert.True(t, limit.tryAcquire())
	assert.False(t, limit.tryAcquire(), "the third concurrent request must be refused")

	limit.release()
	assert.True(t, limit.tryAcquire(), "a released slot must be reusable")
}

func TestStaticAppUsesItsOwnTransport(t *testing.T) {
	t.Parallel()

	shared := &http.Transport{}
	static := &http.Transport{}
	manager := &Manager{transport: shared, staticTransport: static}

	assert.Same(t, static, (&AppUpstream{manager: manager, static: true}).transport())
	assert.Same(t, shared, (&AppUpstream{manager: manager}).transport())
}

func TestStripCORSHeaders(t *testing.T) {
	t.Parallel()

	res := &http.Response{Header: http.Header{}}
	res.Header.Set("Access-Control-Allow-Origin", "https://other-456.hub.keboola.local")
	res.Header.Set("Access-Control-Allow-Credentials", "true")
	res.Header.Set("Content-Type", "text/html")

	stripCORSHeaders(res)

	assert.Equal(t, http.Header{"Content-Type": {"text/html"}}, res.Header)
}
