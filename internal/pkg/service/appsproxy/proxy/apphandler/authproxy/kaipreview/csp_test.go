package kaipreview

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteFrameAncestorsCSP(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	WriteFrameAncestorsCSP(w, []string{"https://connection.keboola.com", "https://connection.eu-central-1.keboola.com"})
	assert.Equal(t,
		"frame-ancestors https://connection.keboola.com https://connection.eu-central-1.keboola.com",
		w.Header().Get("Content-Security-Policy"),
	)
}

func TestWriteFrameAncestorsCSP_Empty(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	WriteFrameAncestorsCSP(w, nil)
	assert.Equal(t, "frame-ancestors 'none'", w.Header().Get("Content-Security-Policy"))
}
