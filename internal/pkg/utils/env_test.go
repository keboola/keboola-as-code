package utils

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMustGetEnvFound(t *testing.T) {
	os.Clearenv()
	assert.NoError(t, os.Setenv("foo", "bar"))
	assert.Equal(t, "bar", MustGetEnv("foo"))
}

func TestMustGetEnvNotFound(t *testing.T) {
	os.Clearenv()
	assert.PanicsWithError(t, `missing ENV variable "foo"`, func() {
		MustGetEnv("foo")
	})
}
