package preview_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview"
)

func TestNormalizeOrigin(t *testing.T) {
	t.Parallel()
	valid := []struct{ in, want string }{
		{"https://my-app-123.hub.keboola.com", "https://my-app-123.hub.keboola.com"},
		{"https://my-app-123.hub.keboola.com/", "https://my-app-123.hub.keboola.com"},
		{"HTTPS://MY-APP-123.HUB.KEBOOLA.COM", "https://my-app-123.hub.keboola.com"},
		{"https://my-app-123.hub.keboola.com:443", "https://my-app-123.hub.keboola.com"},
		{"http://localhost:80", "http://localhost"},
		{"https://basic-auth.hub.keboola.local:18443", "https://basic-auth.hub.keboola.local:18443"},
	}
	for _, tc := range valid {
		got, err := preview.NormalizeOrigin(tc.in)
		require.NoError(t, err, tc.in)
		assert.Equal(t, tc.want, got, tc.in)
	}

	invalid := []string{
		"",
		"my-app-123.hub.keboola.com",
		"ftp://my-app-123.hub.keboola.com",
		"https://my-app-123.hub.keboola.com/x",
		"https://my-app-123.hub.keboola.com?a=b",
		"https://my-app-123.hub.keboola.com#t=x",
		"https://user@my-app-123.hub.keboola.com",
		"https://",
	}
	for _, in := range invalid {
		_, err := preview.NormalizeOrigin(in)
		require.Error(t, err, in)
	}

	a, _ := preview.NormalizeOrigin("https://a.hub.keboola.com:8443")
	b, _ := preview.NormalizeOrigin("https://a.hub.keboola.com")
	assert.NotEqual(t, a, b)
	c, _ := preview.NormalizeOrigin("http://a.hub.keboola.com")
	assert.NotEqual(t, b, c)
}
