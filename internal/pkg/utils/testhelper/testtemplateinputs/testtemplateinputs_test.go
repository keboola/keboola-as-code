package testtemplateinputs

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewUserErrorWithCode(t *testing.T) {
	t.Parallel()

	require.NoError(t, os.Setenv("CUSTOM_ENV", "val1"))      //nolint:forbidigo
	require.NoError(t, os.Setenv("KBC_SECRET_VAR2", "val2")) //nolint:forbidigo
	require.NoError(t, os.Setenv("KBC_SECRET_VAR3", "val3")) //nolint:forbidigo

	provider, err := CreateTestInputsEnvProvider(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "val1", provider.MustGet("CUSTOM_ENV"))
	assert.Equal(t, "val2", provider.MustGet("KBC_SECRET_VAR2"))
	assert.Equal(t, "val3", provider.MustGet("KBC_SECRET_VAR3"))
}
