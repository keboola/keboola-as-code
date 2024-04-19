package requtil

import (
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHost(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "", Host(httptest.NewRequest(http.MethodGet, "/", nil)))
	assert.Equal(t, "foo.bar.com", Host(httptest.NewRequest(http.MethodGet, "https://foo.bar.com", nil)))
	assert.Equal(t, "foo.bar.com", Host(httptest.NewRequest(http.MethodGet, "https://foo.bar.com:8000", nil)))
}
