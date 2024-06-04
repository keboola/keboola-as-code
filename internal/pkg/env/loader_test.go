package env

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func TestLoadDotEnv(t *testing.T) {
	t.Parallel()
	// Memory fs
	logger := log.NewDebugLogger()
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(logger))
	ctx := context.Background()

	// Write envs to file
	osEnvs := Empty()
	osEnvs.Set(`FOO1`, `BAR1`)
	osEnvs.Set(`OS_ONLY`, `123`)
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(".env.local", "FOO1=BAR2\nFOO2=BAR2\n")))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(".env", "FOO1=BAZ\nFOO3=BAR3\n")))

	// Load envs
	logger.Truncate()
	envs := LoadDotEnv(context.Background(), logger, osEnvs, fs, []string{"."})

	// Assert
	assert.Equal(t, map[string]string{
		"OS_ONLY": "123",
		"FOO1":    "BAR1",
		"FOO2":    "BAR2",
		"FOO3":    "BAR3",
	}, envs.ToMap())

	expected := `
{"level":"debug","message":"Loaded \".env.local\""}
{"level":"info","message":"Loaded env file \".env.local\"."}
{"level":"debug","message":"Loaded \".env\""}
{"level":"info","message":"Loaded env file \".env\"."}
`
	logger.AssertJSONMessages(t, expected)
}

func TestLoadDotEnv_Invalid(t *testing.T) {
	t.Parallel()
	// Memory fs
	logger := log.NewDebugLogger()
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(logger))

	// Write envs to file
	require.NoError(t, fs.WriteFile(context.Background(), filesystem.NewRawFile(".env.local", "invalid")))

	// Load envs
	logger.Truncate()
	envs := LoadDotEnv(context.Background(), logger, Empty(), fs, []string{"."})

	// Assert
	assert.Equal(t, map[string]string{}, envs.ToMap())
	expected := `
{"level":"debug","message":"Loaded \".env.local\""}
{"level":"warn","message":"cannot parse env file \".env.local\": unexpected character \"\\n\" in variable name near \"invalid\\n\""}
`
	logger.AssertJSONMessages(t, expected)
}
