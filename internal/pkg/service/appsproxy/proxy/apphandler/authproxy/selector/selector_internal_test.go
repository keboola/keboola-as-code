package selector

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsAcceptedCallbackURL(t *testing.T) {
	t.Parallel()

	cases := map[string]bool{
		"":                false,
		"/":               false,
		"/some/path":      true,
		"/_proxyfoo":      true,
		"/_proxy":         false,
		"/_proxy/sign_in": false,
		"/_PROXY/sign_in": false,
		"/a/../_proxy/x":  false,
		"/_proxy;x":       false,
		"/_proxy?x=1":     false,
		"/_proxy#x":       false,
		"/some/path?x=1":  true,
	}
	for callback, accepted := range cases {
		assert.Equal(t, accepted, isAcceptedCallbackURL(callback), callback)
	}
}
