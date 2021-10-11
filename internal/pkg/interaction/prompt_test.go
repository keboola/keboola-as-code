package interaction

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsInteractiveTerminal(t *testing.T) {
	t.Parallel()
	// The tests are run in a non-interactive terminal
	assert.False(t, isInteractiveTerminal())
}

func TestApiHostValidator(t *testing.T) {
	t.Parallel()
	assert.NoError(t, ApiHostValidator("connection.keboola.com"))
	assert.NoError(t, ApiHostValidator("connection.keboola.com/"))
	assert.NoError(t, ApiHostValidator("https://connection.keboola.com"))
	assert.NoError(t, ApiHostValidator("https://connection.keboola.com/"))
	assert.Equal(t, errors.New("value is required"), ApiHostValidator(""))
	assert.Equal(t, errors.New("invalid host"), ApiHostValidator("@#$$%^&%#$&"))
}

func TestRequiredValidator(t *testing.T) {
	t.Parallel()
	assert.NoError(t, ValueRequired("abc"))
	assert.Equal(t, errors.New("value is required"), ValueRequired(""))
}
