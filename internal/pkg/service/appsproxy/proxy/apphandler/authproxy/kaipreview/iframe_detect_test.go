package kaipreview

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsIframeDocumentLoad(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		dest   string
		accept string
		want   bool
	}{
		{"iframe-html", "iframe", "text/html,*/*;q=0.8", true},
		{"frame-html", "frame", "text/html", true},
		{"document-html", "document", "text/html", false}, // top-level navigation, not iframe
		{"xhr", "empty", "application/json", false},
		{"image", "image", "image/*", false},
		{"script", "script", "*/*", false},
		{"iframe-but-json-accept", "iframe", "application/json", false},
		{"no-sec-fetch-headers", "", "text/html", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := httptest.NewRequestWithContext(t.Context(), "GET", "/some/path", nil)
			if tc.dest != "" {
				r.Header.Set("Sec-Fetch-Dest", tc.dest)
			}
			r.Header.Set("Accept", tc.accept)
			assert.Equal(t, tc.want, IsIframeDocumentLoad(r))
		})
	}
}
