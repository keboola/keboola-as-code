package options

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"sort"
	"testing"
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
	temp := t.TempDir()
	path := filepath.Join(temp, ".env")
	file, err := os.Create(path)
	assert.NoError(t, err)

	// No envs
	os.Clearenv()
	assert.Empty(t, os.Environ())

	// Write envs to file
	_, err = file.WriteString("FOO1=BAR1\nFOO2=BAR2\n")
	assert.NoError(t, err)
	err = file.Close()
	assert.NoError(t, err)

	// Load envs
	err = loadDotEnv(temp)
	assert.NoError(t, err)

	// Assert
	actual := os.Environ()
	sort.Strings(actual)
	assert.Equal(t, []string{"FOO1=BAR1", "FOO2=BAR2"}, actual)
	assert.Equal(t, "BAR1", os.Getenv("FOO1"))
	assert.Equal(t, "BAR2", os.Getenv("FOO2"))
}
