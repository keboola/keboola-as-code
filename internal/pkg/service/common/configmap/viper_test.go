package configmap_test

import (
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
)

func TestBindToViper(t *testing.T) {
	t.Parallel()

	v := viper.New()

	flagToField := func(flag *pflag.Flag) (orderedmap.Path, bool) {
		return orderedmap.PathFromStr(flag.Name), true
	}

	fs := pflag.NewFlagSet("app", pflag.ContinueOnError)
	fs.Bool("verbose", false, "")
	fs.String("foo", "foo", "")
	fs.String("foo-bar", "bar", "")
	require.NoError(t, fs.Parse([]string{"app", "--foo", "from flag"}))

	envs := env.Empty()
	envs.Set("MY_APP_FOO_BAR", "from env")

	// Bind
	setBy, err := configmap.BindToViper(v, flagToField, configmap.BindConfig{Flags: fs, Envs: envs, EnvNaming: env.NewNamingConvention("MY_APP_")})
	require.NoError(t, err)
	assert.Equal(t, map[string]configmap.SetBy{
		"verbose": configmap.SetByDefault,
		"foo":     configmap.SetByFlag,
		"foo-bar": configmap.SetByEnv,
	}, setBy)
	assert.False(t, v.IsSet("verbose"))
	assert.False(t, v.GetBool("verbose"))
	assert.True(t, v.IsSet("foo"))
	assert.Equal(t, "from flag", v.GetString("foo"))
	assert.True(t, v.IsSet("foo-bar"))
	assert.Equal(t, "from env", v.GetString("foo-bar"))
}
