package interaction

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIsInteractiveTerminal(t *testing.T) {
	// The tests are run in a non-interactive terminal
	assert.False(t, isInteractiveTerminal())
}

func TestApiHostValidator(t *testing.T) {
	assert.NoError(t, ApiHostValidator("connection.keboola.com"))
	assert.NoError(t, ApiHostValidator("connection.keboola.com/"))
	assert.NoError(t, ApiHostValidator("https://connection.keboola.com"))
	assert.NoError(t, ApiHostValidator("https://connection.keboola.com/"))
	assert.Equal(t, errors.New("value is required"), ApiHostValidator(""))
	assert.Equal(t, errors.New("invalid host"), ApiHostValidator("@#$$%^&%#$&"))
}

func TestRequiredValidator(t *testing.T) {
	assert.NoError(t, ValueRequired("abc"))
	assert.Equal(t, errors.New("value is required"), ValueRequired(""))
}
