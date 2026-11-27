package cookiemigration_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/encryption"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/cookiemigration"
)

const secret = "0123456789abcdef0123456789abcdef"

func newRequest(t *testing.T, cookies ...*http.Cookie) *http.Request {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://app.example.com/", nil)
	for _, c := range cookies {
		req.AddCookie(c)
	}
	return req
}

func TestCopy(t *testing.T) {
	t.Parallel()

	t.Run("copies when target is missing", func(t *testing.T) {
		t.Parallel()
		req := newRequest(t, &http.Cookie{Name: "new", Value: "v"})
		out := cookiemigration.Copy(req, "new", "old")
		c, err := out.Cookie("old")
		require.NoError(t, err)
		assert.Equal(t, "v", c.Value)
		_, err = req.Cookie("old")
		require.Error(t, err, "the original request is not modified")
	})

	t.Run("target wins when both are present", func(t *testing.T) {
		t.Parallel()
		req := newRequest(t, &http.Cookie{Name: "new", Value: "n"}, &http.Cookie{Name: "old", Value: "o"})
		out := cookiemigration.Copy(req, "new", "old")
		assert.Same(t, req, out)
		c, _ := out.Cookie("old")
		assert.Equal(t, "o", c.Value)
	})

	t.Run("nothing to copy", func(t *testing.T) {
		t.Parallel()
		req := newRequest(t)
		assert.Same(t, req, cookiemigration.Copy(req, "new", "old"))
	})
}

func signed(t *testing.T, name string, value []byte, at time.Time) *http.Cookie {
	t.Helper()
	v, err := encryption.SignedValue(secret, name, value, at)
	require.NoError(t, err)
	return &http.Cookie{Name: name, Value: v}
}

func TestResign(t *testing.T) {
	t.Parallel()

	expire := 24 * time.Hour

	t.Run("re-signed cookie validates under the target name and keeps its timestamp", func(t *testing.T) {
		t.Parallel()
		issuedAt := time.Now().Add(-23 * time.Hour).Truncate(time.Second)
		req := newRequest(t, signed(t, "new", []byte("ticket"), issuedAt))

		out, ok := cookiemigration.Resign(req, "new", "old", secret, expire)
		require.True(t, ok)
		c, err := out.Cookie("old")
		require.NoError(t, err)
		value, ts, valid := encryption.Validate(c, secret, expire)
		require.True(t, valid)
		assert.Equal(t, []byte("ticket"), value)
		assert.Equal(t, issuedAt, ts)
	})

	t.Run("works in both directions", func(t *testing.T) {
		t.Parallel()
		issuedAt := time.Now().Add(-time.Hour).Truncate(time.Second)
		req := newRequest(t, signed(t, "old", []byte("ticket"), issuedAt))

		out, ok := cookiemigration.Resign(req, "old", "new", secret, expire)
		require.True(t, ok)
		c, err := out.Cookie("new")
		require.NoError(t, err)
		_, ts, valid := encryption.Validate(c, secret, expire)
		require.True(t, valid)
		assert.Equal(t, issuedAt, ts)
	})

	t.Run("a plain copy does not validate", func(t *testing.T) {
		t.Parallel()
		c := signed(t, "new", []byte("ticket"), time.Now())
		_, _, valid := encryption.Validate(&http.Cookie{Name: "old", Value: c.Value}, secret, expire)
		assert.False(t, valid)
	})

	t.Run("expired cookie is not translated", func(t *testing.T) {
		t.Parallel()
		req := newRequest(t, signed(t, "new", []byte("ticket"), time.Now().Add(-25*time.Hour)))
		out, ok := cookiemigration.Resign(req, "new", "old", secret, expire)
		assert.False(t, ok)
		assert.Same(t, req, out)
	})

	t.Run("wrong secret is not translated", func(t *testing.T) {
		t.Parallel()
		req := newRequest(t, signed(t, "new", []byte("ticket"), time.Now()))
		_, ok := cookiemigration.Resign(req, "new", "old", strings.Repeat("x", 32), expire)
		assert.False(t, ok)
	})

	t.Run("a cookie signed for another name is not translated", func(t *testing.T) {
		t.Parallel()
		c := signed(t, "other", []byte("ticket"), time.Now())
		c.Name = "new"
		_, ok := cookiemigration.Resign(newRequest(t, c), "new", "old", secret, expire)
		assert.False(t, ok)
	})

	t.Run("target wins when both are present", func(t *testing.T) {
		t.Parallel()
		req := newRequest(t, signed(t, "new", []byte("n"), time.Now()), signed(t, "old", []byte("o"), time.Now()))
		out, ok := cookiemigration.Resign(req, "new", "old", secret, expire)
		assert.False(t, ok)
		assert.Same(t, req, out)
	})
}
