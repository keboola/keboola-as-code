package input

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIf_Evaluate(t *testing.T) {
	t.Parallel()

	// Empty
	result, err := If("").Evaluate(map[string]any{})
	assert.True(t, result)
	require.NoError(t, err)

	// Simple
	result, err = If("true").Evaluate(map[string]any{})
	assert.True(t, result)
	require.NoError(t, err)

	// Parameter - true
	result, err = If("[my-param]").Evaluate(map[string]any{"my-param": true})
	assert.True(t, result)
	require.NoError(t, err)

	// Parameter - false
	result, err = If("[my-param]").Evaluate(map[string]any{"my-param": false})
	assert.False(t, result)
	require.NoError(t, err)

	// Parameter - not found
	result, err = If("[my-param]").Evaluate(map[string]any{})
	assert.False(t, result)
	require.Error(t, err)
	assert.Equal(t, "cannot evaluate condition:\n- expression: [my-param]\n- error: No parameter 'my-param' found.", err.Error())

	// Invalid expression
	result, err = If(">>>>>").Evaluate(map[string]any{})
	assert.False(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot compile condition")
	assert.Contains(t, err.Error(), "expression: >>>>>")
}
