package env

import (
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
	fs, err := aferofs.NewMemoryFs(logger, ".")
	assert.NoError(t, err)

	// Write envs to file
	osEnvs := Empty()
	osEnvs.Set(`FOO1`, `BAR1`)
	osEnvs.Set(`OS_ONLY`, `123`)
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(".env.local", "FOO1=BAR2\nFOO2=BAR2\n")))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(".env", "FOO1=BAZ\nFOO3=BAR3\n")))

	// Load envs
	envs, err := LoadDotEnv(logger, osEnvs, fs, []string{"."})
	assert.NoError(t, err)

	// Assert
	assert.Equal(t, map[string]string{
		"OS_ONLY": "123",
		"FOO1":    "BAR1",
		"FOO2":    "BAR2",
		"FOO3":    "BAR3",
	}, envs.ToMap())

	assert.Equal(t, `DEBUG  Saved ".env.local"
DEBUG  Saved ".env"
DEBUG  Loaded ".env.local"
INFO  Loaded env file ".env.local"
DEBUG  Loaded ".env"
INFO  Loaded env file ".env"
`, logger.AllMessages(),
	)
}
