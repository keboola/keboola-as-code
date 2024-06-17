package stream_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream"
)

func TestParseComponentsList(t *testing.T) {
	t.Parallel()

	// Nil
	_, err := stream.ParseComponentsList(nil)
	if assert.Error(t, err) {
		assert.Equal(t, "specify at least one service component as a positional argument", err.Error())
	}

	// Empty
	_, err = stream.ParseComponentsList([]string{})
	if assert.Error(t, err) {
		assert.Equal(t, "specify at least one service component as a positional argument", err.Error())
	}

	// Binary name only
	_, err = stream.ParseComponentsList([]string{"app"})
	if assert.Error(t, err) {
		assert.Equal(t, "specify at least one service component as a positional argument", err.Error())
	}

	// Unexpected
	_, err = stream.ParseComponentsList([]string{"app", "foo", "bar"})
	if assert.Error(t, err) {
		assert.Equal(t, `unexpected service component: "foo", "bar"`, err.Error())
	}

	// OK
	components, err := stream.ParseComponentsList([]string{"app", string(stream.ComponentAPI)})
	assert.NoError(t, err)
	assert.Equal(t, stream.Components{stream.ComponentAPI}, components)
}
