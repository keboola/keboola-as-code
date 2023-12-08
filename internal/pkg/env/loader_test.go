package env

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func TestLoadDotEnv(t *testing.T) {
	t.Parallel()
	// Memory fs
	logger := log.NewDebugLogger()
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(logger))

	// Write envs to file
	osEnvs := Empty()
	osEnvs.Set(`FOO1`, `BAR1`)
	osEnvs.Set(`OS_ONLY`, `123`)
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(".env.local", "FOO1=BAR2\nFOO2=BAR2\n")))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(".env", "FOO1=BAZ\nFOO3=BAR3\n")))

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
DEBUG  Loaded ".env.local"
INFO  Loaded env file ".env.local".
DEBUG  Loaded ".env"
INFO  Loaded env file ".env".
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), logger.AllMessages())
}

func TestLoadDotEnv_Invalid(t *testing.T) {
	t.Parallel()
	// Memory fs
	logger := log.NewDebugLogger()
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(logger))

	// Write envs to file
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(".env.local", "invalid")))

	// Load envs
	logger.Truncate()
	envs := LoadDotEnv(context.Background(), logger, Empty(), fs, []string{"."})

	// Assert
	assert.Equal(t, map[string]string{}, envs.ToMap())
	expected := `
DEBUG  Loaded ".env.local"
WARN  cannot parse env file ".env.local": unexpected character "\n" in variable name near "invalid\n"
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), logger.AllMessages())
}
