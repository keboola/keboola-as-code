package options

import (
	"strings"
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
	assert.Equal(t, "", options.ApiHost)

	// 1. Lowest priority, ".env" file from project dir
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(".env", "KBC_STORAGE_API_TOKEN=1abcdef")))
	err = options.Load(logger, env.Empty(), fs, flags)
	assert.NoError(t, err)
	assert.Equal(t, "1abcdef", options.ApiToken)

	// 2. Higher priority, ".env" file from working dir
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(workingDir, ".env"), "KBC_STORAGE_API_TOKEN=2abcdef")))
	err = options.Load(logger, env.Empty(), fs, flags)
	assert.NoError(t, err)
	assert.Equal(t, "2abcdef", options.ApiToken)

	// 3. Higher priority , ENV defined in OS
	osEnvs := env.Empty()
	osEnvs.Set("KBC_STORAGE_API_TOKEN", "3abcdef")
	err = options.Load(logger, osEnvs, fs, flags)
	assert.NoError(t, err)
	assert.Equal(t, "3abcdef", options.ApiToken)

	// 4. The highest priority, flag
	assert.NoError(t, flags.Set("storage-api-token", "4abcdef"))
	err = options.Load(logger, osEnvs, fs, flags)
	assert.NoError(t, err)
	assert.Equal(t, "4abcdef", options.ApiToken)
}

func TestValidateNoRequired(t *testing.T) {
	t.Parallel()
	options := NewOptions()
	assert.Empty(t, options.Validate([]string{}))
}

func TestValidateAllRequired(t *testing.T) {
	t.Parallel()
	options := NewOptions()
	errors := options.Validate([]string{"ApiHost", "ApiToken"})

	// Assert
	expected := []string{
		`- Missing api host. Please use "--storage-api-host" flag or ENV variable "KBC_STORAGE_API_HOST".`,
		`- Missing api token. Please use "--storage-api-token" flag or ENV variable "KBC_STORAGE_API_TOKEN".`,
	}
	assert.Equal(t, strings.Join(expected, "\n"), errors)
}

func TestDumpOptions(t *testing.T) {
	t.Parallel()
	options := NewOptions()
	options.ApiHost = "connection.keboola.com"
	options.ApiToken = "12345-67890123abcd"
	expected := `Parsed options: {"Verbose":false,"VerboseApi":false,"LogFilePath":"","ApiHost":"connection.keboola.com","ApiToken":"12345-6*****"}`
	assert.Equal(t, expected, options.Dump())
}
