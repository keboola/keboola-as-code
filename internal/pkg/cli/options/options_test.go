package options

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
)

func TestValuesPriority(t *testing.T) {
	t.Parallel()
	logger := zap.NewNop().Sugar()
	workingDir := filesystem.Join("foo", "bar")
	fs, err := aferofs.NewMemoryFs(logger, workingDir)
	assert.NoError(t, err)

	// Create working and project dir
	assert.NoError(t, fs.Mkdir(workingDir))

	// Create structs
	flags := &pflag.FlagSet{}
	flags.String("storage-api-token", "", "")
	options := NewOptions()

	// No values defined
	err = options.Load(logger, env.Empty(), fs, flags)
	assert.NoError(t, err)
	assert.Equal(t, "", options.GetString(`storage-api-token`))

	// 1. Lowest priority, ".env" file from project dir
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(".env", "KBC_STORAGE_API_TOKEN=1abcdef")))
	err = options.Load(logger, env.Empty(), fs, flags)
	assert.NoError(t, err)
	assert.Equal(t, "1abcdef", options.GetString(`storage-api-token`))

	// 2. Higher priority, ".env" file from working dir
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(workingDir, ".env"), "KBC_STORAGE_API_TOKEN=2abcdef")))
	err = options.Load(logger, env.Empty(), fs, flags)
	assert.NoError(t, err)
	assert.Equal(t, "2abcdef", options.GetString(`storage-api-token`))

	// 3. Higher priority , ENV defined in OS
	osEnvs := env.Empty()
	osEnvs.Set("KBC_STORAGE_API_TOKEN", "3abcdef")
	err = options.Load(logger, osEnvs, fs, flags)
	assert.NoError(t, err)
	assert.Equal(t, "3abcdef", options.GetString(`storage-api-token`))

	// 4. The highest priority, flag
	assert.NoError(t, flags.Set("storage-api-token", "4abcdef"))
	err = options.Load(logger, osEnvs, fs, flags)
	assert.NoError(t, err)
	assert.Equal(t, "4abcdef", options.GetString(`storage-api-token`))
}

func TestDumpOptions(t *testing.T) {
	t.Parallel()
	options := NewOptions()
	options.Set(`storage-api-host`, "connection.keboola.com")
	options.Set(`storage-api-token`, "12345-67890123abcd")
	expected := "Parsed options:\n  storage-api-host = \"connection.keboola.com\"\n  storage-api-token = \"12345-6*****\"\n"
	assert.Equal(t, expected, options.Dump())
}
