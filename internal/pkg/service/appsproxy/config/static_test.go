package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
)

func TestIsInternalPath(t *testing.T) {
	t.Parallel()

	cases := []struct {
		path     string
		internal bool
	}{
		{"/_proxy", true},
		{"/_proxy/", true},
		{"/_proxy/sign_in", true},
		{"/_proxy/static/css/x.css", true},
		{"//_proxy/x", true},
		{"/a/../_proxy/x", true},
		{"/_PROXY/x", true},
		{"/_Proxy", true},
		{"/_proxy;x", true},
		{"/_proxy;x/y", true},
		{"/;x/_proxy/y", true},
		{"/", false},
		{"/_proxyfoo", false},
		{"/_proxyfoo/x", false},
		{"/app/_proxy/x", false},
		{"/app;_proxy/x", false},
	}
	for _, c := range cases {
		assert.Equal(t, c.internal, config.IsInternalPath(c.path), c.path)
	}
}
