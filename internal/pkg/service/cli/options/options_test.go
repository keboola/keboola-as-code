package options

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
)

func TestValuesPriority(t *testing.T) {
	t.Parallel()
	logger := log.NewNopLogger()
	workingDir := filesystem.Join("foo", "bar")
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(logger), filesystem.WithWorkingDir(workingDir))

	// Create working and project dir
	assert.NoError(t, fs.Mkdir(workingDir))

	key := "storage-api-token"

	// 1. Key is not defined
	options := New()
	err := options.Load(logger, env.Empty(), fs, &pflag.FlagSet{})
	assert.NoError(t, err)
	assert.Equal(t, "", options.GetString(key))
	assert.Equal(t, configmap.SetByUnknown, options.KeySetBy(key))

	// 2. Lowest priority, flag default value
	flags := &pflag.FlagSet{}
	flags.String(key, "default flag value", "")
	options = New()
	err = options.Load(logger, env.Empty(), fs, flags)
	assert.NoError(t, err)
	assert.Equal(t, "default flag value", options.GetString(key))
	assert.Equal(t, configmap.SetByDefault, options.KeySetBy(key))

	// 3. Higher priority, ".env" file from project dir
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(".env", "KBC_STORAGE_API_TOKEN=1abcdef")))
	options = New()
	err = options.Load(logger, env.Empty(), fs, flags)
	assert.NoError(t, err)
	assert.Equal(t, "1abcdef", options.GetString(key))
	assert.Equal(t, configmap.SetByEnv, options.KeySetBy(key))

	// 4. Higher priority, ".env" file from working dir
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(workingDir, ".env"), "KBC_STORAGE_API_TOKEN=2abcdef")))
	options = New()
	err = options.Load(logger, env.Empty(), fs, flags)
	assert.NoError(t, err)
	assert.Equal(t, "2abcdef", options.GetString(key))
	assert.Equal(t, configmap.SetByEnv, options.KeySetBy(key))

	// 5. Higher priority , ENV defined in OS
	osEnvs := env.Empty()
	osEnvs.Set("KBC_STORAGE_API_TOKEN", "3abcdef")
	options = New()
	err = options.Load(logger, osEnvs, fs, flags)
	assert.NoError(t, err)
	assert.Equal(t, "3abcdef", options.GetString(key))
	assert.Equal(t, configmap.SetByEnv, options.KeySetBy(key))

	// 6. Higher priority , flag value
	assert.NoError(t, flags.Set(key, "4abcdef"))
	options = New()
	err = options.Load(logger, osEnvs, fs, flags)
	assert.NoError(t, err)
	assert.Equal(t, "4abcdef", options.GetString(key))
	assert.Equal(t, configmap.SetByFlag, options.KeySetBy(key))

	// 7. The highest priority, Set method
	options = New()
	err = options.Load(logger, osEnvs, fs, flags)
	options.Set(key, "foo-bar")
	assert.NoError(t, err)
	assert.Equal(t, "foo-bar", options.GetString(key))
	assert.Equal(t, configmap.SetManually, options.KeySetBy(key))
}

func TestDumpOptions(t *testing.T) {
	t.Parallel()
	options := New()
	options.Set(`storage-api-host`, "connection.keboola.com")
	options.Set("storage-api-token", "12345-67890123abcd")
	expected := "Parsed options:\n  storage-api-host = \"connection.keboola.com\"\n  storage-api-token = \"12345-6*****\"\n"
	assert.Equal(t, expected, options.Dump())
}
