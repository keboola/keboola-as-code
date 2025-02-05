package ip

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFrom(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "<nil>", From(nil).String())
	assert.Equal(t, "<nil>", From(&http.Request{}).String())
	assert.Equal(t, "1.1.1.1", From(&http.Request{RemoteAddr: "1.1.1.1:1234"}).String())
	assert.Equal(t, "::1", From(&http.Request{RemoteAddr: "[::1]:1234"}).String())

	h := make(http.Header)
	h.Add(XRealIPHeader, "1.1.1.2")
	assert.Equal(t, "1.1.1.2", From(&http.Request{Header: h, RemoteAddr: "1.1.1.1:1234"}).String())

	h = make(http.Header)
	h.Add(XRealIPHeader, "::1")
	assert.Equal(t, "::1", From(&http.Request{Header: h, RemoteAddr: "1.1.1.1:1234"}).String())

	h = make(http.Header)
	h.Add(XForwardedForHeader, "1.1.1.2")
	assert.Equal(t, "1.1.1.2", From(&http.Request{Header: h, RemoteAddr: "1.1.1.1:1234"}).String())

	h = make(http.Header)
	h.Add(XForwardedForHeader, "::1")
	assert.Equal(t, "::1", From(&http.Request{Header: h, RemoteAddr: "1.1.1.1:1234"}).String())

	h = make(http.Header)
	h.Add(XForwardedForHeader, "1.1.1.3:53000")
	assert.Equal(t, "1.1.1.3", From(&http.Request{Header: h, RemoteAddr: "1.1.1.1:1234"}).String())
}
