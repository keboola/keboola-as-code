package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatUrl(t *testing.T) {
	t.Parallel()

	assert.Equal(
		t,
		"https://buffer.keboola.local/v1/import/1000/asdf/#/fdsa",
		formatUrl("buffer.keboola.local", 1000, "asdf", "fdsa"),
	)
}
