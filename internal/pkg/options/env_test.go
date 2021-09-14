package options

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"keboola-as-code/src/utils"
)

func TestEnvNamingConvention(t *testing.T) {
	c := &envNamingConvention{}
	assert.Equal(t, "KBC_FOO", c.Replace("foo"))
	assert.Equal(t, "KBC_FOO_BAR", c.Replace("foo-bar"))
	assert.Equal(t, "KBC_FOO_BAR_BAZ", c.Replace("foo-Bar-BAZ"))
}

func TestEnvNamingConventionFlagNameEmpty(t *testing.T) {
	c := &envNamingConvention{}
	assert.PanicsWithError(t, "flag name cannot be empty", func() {
		c.Replace("")
	})
}

func TestLoadDotEnv(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	temp := t.TempDir()

	// No envs
	os.Clearenv()
	assert.Empty(t, os.Environ())

	// Write envs to file
	assert.NoError(t, os.WriteFile(filepath.Join(temp, ".env.local"), []byte("FOO1=BAR1\nFOO2=BAR2\n"), 0650))
	assert.NoError(t, os.WriteFile(filepath.Join(temp, ".env"), []byte("FOO1=BAZ\nFOO3=BAR3\n"), 0644))

	// Load envs
	err := loadDotEnv(temp)
	assert.NoError(t, err)

	// Assert
	actual := os.Environ()
	sort.Strings(actual)
	assert.Equal(t, []string{"FOO1=BAR1", "FOO2=BAR2", "FOO3=BAR3"}, actual)
	assert.Equal(t, "BAR1", os.Getenv("FOO1"))
	assert.Equal(t, "BAR2", os.Getenv("FOO2"))
	assert.Equal(t, "BAR3", os.Getenv("FOO3"))
}
