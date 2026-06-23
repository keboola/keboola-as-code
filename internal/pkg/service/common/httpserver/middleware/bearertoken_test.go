package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBearerToken_extract(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "kbc_at_x", bearerToken("Bearer kbc_at_x"))
	assert.Equal(t, "kbc_at_x", bearerToken("bearer kbc_at_x")) // case-insensitive scheme
	assert.Equal(t, "kbc_at_x", bearerToken("Bearer  kbc_at_x ")) // trims surrounding spaces
	assert.Empty(t, bearerToken(""))
	assert.Empty(t, bearerToken("Bearer"))
	assert.Empty(t, bearerToken("Basic abc"))
}

func TestBearerToken_promotesToHeader(t *testing.T) {
	t.Parallel()
	const tokenHeader = "X-StorageAPI-Token"

	run := func(setup func(*http.Request)) string {
		var seen string
		h := BearerToken(tokenHeader)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			seen = r.Header.Get(tokenHeader)
		}))
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		setup(req)
		h.ServeHTTP(httptest.NewRecorder(), req)
		return seen
	}

	// Bearer token is promoted into the storage-token header.
	assert.Equal(t, "kbc_at_x", run(func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer kbc_at_x")
	}))

	// Existing storage-token header is not overwritten.
	assert.Equal(t, "explicit", run(func(r *http.Request) {
		r.Header.Set(tokenHeader, "explicit")
		r.Header.Set("Authorization", "Bearer kbc_at_x")
	}))

	// No Authorization, no header -> stays empty.
	assert.Empty(t, run(func(_ *http.Request) {}))
}
