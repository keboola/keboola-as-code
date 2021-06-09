package cli

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIsInteractiveTerminal(t *testing.T) {
	// The tests are run in a non-interactive terminal
	assert.False(t, isInteractiveTerminal())
}
