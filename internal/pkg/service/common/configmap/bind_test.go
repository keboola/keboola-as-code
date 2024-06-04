package configmap

import (
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// TestGenerateAndBind_Empty tests empty binding without default values.
func TestGenerateAndBind_Empty(t *testing.T) {
	t.Parallel()

	cfg := GenerateAndBindConfig{
		Args:      []string(nil),
		EnvNaming: env.NewNamingConvention("MY_APP_"),
		Envs:      env.Empty(),
	}

	target := TestConfig{}

	require.NoError(t, GenerateAndBind(cfg, &target))
	assert.Equal(t, TestConfig{}, target)
}

// TestGenerateAndBind_NormalizeAndValidate tests the Normalize and Validate method calls if a type implements them.
func TestGenerateAndBind_NormalizeAndValidate(t *testing.T) {
	t.Parallel()

	cfg := GenerateAndBindConfig{
		Args:      []string{"--key1-foo", "  Foo  "},
		EnvNaming: env.NewNamingConvention("MY_APP_"),
		Envs:      env.Empty(),
	}

	target := TestConfigNV{}
	target.ValidationError = errors.New("config error")
	target.Key1 = TestValueNV{ValidationError: errors.New("key1 error")}
	target.Key2.KeyB = TestValueNV{ValidationError: errors.New("keyB error")}

	err := GenerateAndBind(cfg, &target)
	if assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`
configuration is not valid:
- config error
- invalid "key1": key1 error
- invalid "key2.keyB": keyB error
- "key2.keyA" is a required field
`), err.Error())
		assert.Equal(t, "Foo", target.Key1.Foo)          // normalized
		assert.Equal(t, "normalized", target.Normalized) // set by the Normalize method
	}
}

// TestGenerateAndBind_Default tests empty binding with default values.
func TestGenerateAndBind_DefaultValues(t *testing.T) {
	t.Parallel()

	cfg := GenerateAndBindConfig{
		Args:                   []string(nil),
		EnvNaming:              env.NewNamingConvention("MY_APP_"),
		Envs:                   env.Empty(),
		GenerateHelpFlag:       true,
		GenerateConfigFileFlag: true,
		GenerateDumpConfigFlag: true,
	}

	duration := 123 * time.Second
	addrValue := netip.AddrFrom4([4]byte{1, 2, 3, 4})
	expected := TestConfig{
		Embedded: Embedded{
			EmbeddedField: "default Value",
		},
		SensitiveString: "value1",
		Int:             123,
		Float:           4.56,
		Nested: Nested{
			Foo: "foo",
			Bar: 789,
		},
		StringWithUsage:  "value2",
		Duration:         duration,
		DurationNullable: &duration,
		URL:              &url.URL{Scheme: "http", Host: "localhost:1234"},
		Addr:             addrValue,
		AddrNullable:     &addrValue,
	}
	target := expected

	// TestConfig is unmodified, default values are kept
	require.NoError(t, GenerateAndBind(cfg, &target))
	assert.NotSame(t, expected, target)
	assert.Equal(t, expected, target)
}

// TestGenerateAndBind_Flags tests binding from flags to the configuration structure.
func TestGenerateAndBind_Flags(t *testing.T) {
	t.Parallel()

	var posArgs []string
	cfg := GenerateAndBindConfig{
		Args: []string{
			"--embedded", "foo",
			"--sensitive-string", "abc",
			"--int", "1000",
			"--float", "78.90",
			"--nested-foo", "def",
			"--nested-bar", "2000",
			"--string-with-usage", "",
			"--duration", "100s",
			"--duration-nullable", "100s",
			"--url", "https://foo.bar",
			"--address", "10.20.30.40",
			"--address-nullable", "10.20.30.40",
			"pos1", "pos2", "pos3",
		},
		EnvNaming:              env.NewNamingConvention("MY_APP_"),
		Envs:                   env.Empty(),
		PositionalArgsTarget:   &posArgs,
		GenerateHelpFlag:       true,
		GenerateConfigFileFlag: true,
		GenerateDumpConfigFlag: true,
	}

	defaultDuration := 123 * time.Second
	defaultAddrValue := netip.AddrFrom4([4]byte{1, 2, 3, 4})
	target := TestConfig{
		Embedded: Embedded{
			EmbeddedField: "default Value",
		},
		SensitiveString: "value1",
		Int:             123,
		Float:           4.56,
		Nested: Nested{
			Foo: "foo",
			Bar: 789,
		},
		StringWithUsage:  "value2",
		Duration:         defaultDuration,
		DurationNullable: &defaultDuration,
		URL:              &url.URL{Scheme: "http", Host: "localhost:1234"},
		Addr:             defaultAddrValue,
		AddrNullable:     &defaultAddrValue,
	}

	// Default values are replaced from flags
	expectedDuration := 100 * time.Second
	expectedAddrValue := netip.AddrFrom4([4]byte{10, 20, 30, 40})
	require.NoError(t, GenerateAndBind(cfg, &target))
	assert.Equal(t, []string{"pos1", "pos2", "pos3"}, posArgs)
	assert.Equal(t, TestConfig{
		Embedded:        Embedded{EmbeddedField: "foo"},
		SensitiveString: "abc",
		Int:             1000,
		Float:           78.90,
		Nested: Nested{
			Foo: "def",
			Bar: 2000,
		},
		StringWithUsage:  "",
		Duration:         expectedDuration,
		DurationNullable: &expectedDuration,
		URL:              &url.URL{Scheme: "https", Host: "foo.bar"},
		Addr:             expectedAddrValue,
		AddrNullable:     &expectedAddrValue,
	}, target)
}

// TestGenerateAndBind_Env tests binding from ENVs to the configuration structure.
func TestGenerateAndBind_Env(t *testing.T) {
	t.Parallel()

	envs := env.Empty()
	envs.Set("MY_APP_EMBEDDED", "foo")
	envs.Set("MY_APP_SENSITIVE_STRING", "abc")
	envs.Set("MY_APP_INT", "1000")
	envs.Set("MY_APP_FLOAT", "78.90")
	envs.Set("MY_APP_NESTED_FOO", "def")
	envs.Set("MY_APP_NESTED_BAR", "2000")
	envs.Set("MY_APP_STRING_WITH_USAGE", "")
	envs.Set("MY_APP_DURATION", "100s")
	envs.Set("MY_APP_DURATION_NULLABLE", "100s")
	envs.Set("MY_APP_ADDRESS", "10.20.30.40")
	envs.Set("MY_APP_ADDRESS_NULLABLE", "10.20.30.40")

	cfg := GenerateAndBindConfig{
		Args: []string{
			"--embedded", "flag takes precedence over ENV",
		},
		EnvNaming:              env.NewNamingConvention("MY_APP_"),
		Envs:                   envs,
		GenerateHelpFlag:       true,
		GenerateConfigFileFlag: true,
		GenerateDumpConfigFlag: true,
	}

	defaultDuration := 123 * time.Second
	defaultAddrValue := netip.AddrFrom4([4]byte{1, 2, 3, 4})
	target := TestConfig{
		Embedded: Embedded{
			EmbeddedField: "default Value",
		},
		SensitiveString: "value1",
		Int:             123,
		Float:           4.56,
		Nested: Nested{
			Foo: "foo",
			Bar: 789,
		},
		StringWithUsage:  "value2",
		Duration:         defaultDuration,
		DurationNullable: &defaultDuration,
		URL:              &url.URL{Scheme: "http", Host: "localhost:1234"},
		Addr:             defaultAddrValue,
		AddrNullable:     &defaultAddrValue,
	}

	// Default values are replaced from ENVs
	expectedDuration := 100 * time.Second
	expectedAddrValue := netip.AddrFrom4([4]byte{10, 20, 30, 40})
	require.NoError(t, GenerateAndBind(cfg, &target))
	assert.Equal(t, TestConfig{
		Embedded: Embedded{
			EmbeddedField: "flag takes precedence over ENV", // from flag
		},
		SensitiveString: "abc",
		Int:             1000,
		Float:           78.90,
		Nested: Nested{
			Foo: "def",
			Bar: 2000,
		},
		StringWithUsage:  "",
		Duration:         expectedDuration,
		DurationNullable: &expectedDuration,
		URL:              &url.URL{Scheme: "http", Host: "localhost:1234"},
		Addr:             expectedAddrValue,
		AddrNullable:     &expectedAddrValue,
	}, target)
}

// TestGenerateAndBind_ConfigFile_YAML tests binding from YAML config files to the configuration structure.
func TestGenerateAndBind_ConfigFile_YAML(t *testing.T) {
	t.Parallel()

	envs := env.Empty()
	envs.Set("MY_APP_EMBEDDED", "foo")
	envs.Set("MY_APP_SENSITIVE_STRING", "abc")
	envs.Set("MY_APP_FLOAT", "78.90")
	envs.Set("MY_APP_NESTED_BAR", "2000")
	envs.Set("MY_APP_STRING_WITH_USAGE", "")
	envs.Set("MY_APP_DURATION", "100s")
	envs.Set("MY_APP_DURATION_NULLABLE", "100s")

	configFilePath1 := filesystem.Join(t.TempDir(), "config1.yaml")
	configFilePath2 := filesystem.Join(t.TempDir(), "config2.yaml")
	cfg := GenerateAndBindConfig{
		Args: []string{
			"--embedded", "flag takes precedence over ENV",
			"--config-file", configFilePath1,
			"--config-file", configFilePath2,
		},
		EnvNaming:              env.NewNamingConvention("MY_APP_"),
		Envs:                   envs,
		GenerateHelpFlag:       true,
		GenerateConfigFileFlag: true,
		GenerateDumpConfigFlag: true,
	}

	defaultDuration := 123 * time.Second
	defaultAddrValue := netip.AddrFrom4([4]byte{1, 2, 3, 4})
	target := TestConfig{
		Embedded: Embedded{
			EmbeddedField: "default Value",
		},
		SensitiveString: "value1",
		Int:             123,
		Float:           4.56,
		Nested: Nested{
			Foo: "foo",
			Bar: 789,
		},
		StringWithUsage:  "value2",
		Duration:         defaultDuration,
		DurationNullable: &defaultDuration,
		URL:              &url.URL{Scheme: "http", Host: "localhost:1234"},
		Addr:             defaultAddrValue,
		AddrNullable:     &defaultAddrValue,
	}

	// Write YAML config files
	config1 := `
nested:
  foo: from config
  bar: 1000
address: 11.22.33.44
addressNullable: 11.22.33.44
`
	require.NoError(t, os.WriteFile(configFilePath1, []byte(config1), 0o600))
	config2 := `
url: https://foo.bar
int: 999
`
	require.NoError(t, os.WriteFile(configFilePath2, []byte(config2), 0o600))

	// Default values are replaced from the YAML config file.
	require.NoError(t, GenerateAndBind(cfg, &target))
	expectedDuration := 100 * time.Second
	expectedAddrValue := netip.AddrFrom4([4]byte{11, 22, 33, 44})
	assert.Equal(t, TestConfig{
		Embedded: Embedded{
			EmbeddedField: "flag takes precedence over ENV", // from flag
		},
		SensitiveString: "abc",
		Int:             999,
		Float:           78.90,
		Nested: Nested{
			Foo: "from config", // from config
			Bar: 2000,          // from ENV
		},
		StringWithUsage:  "",
		Duration:         expectedDuration,
		DurationNullable: &expectedDuration,
		URL:              &url.URL{Scheme: "https", Host: "foo.bar"},
		Addr:             expectedAddrValue,
		AddrNullable:     &expectedAddrValue,
	}, target)
}

// TestGenerateAndBind_ConfigFile_JSON tests binding from JSON config files to the configuration structure.
func TestGenerateAndBind_ConfigFile_JSON(t *testing.T) {
	t.Parallel()

	envs := env.Empty()
	envs.Set("MY_APP_EMBEDDED", "foo")
	envs.Set("MY_APP_SENSITIVE_STRING", "abc")
	envs.Set("MY_APP_FLOAT", "78.90")
	envs.Set("MY_APP_NESTED_BAR", "2000")
	envs.Set("MY_APP_STRING_WITH_USAGE", "")
	envs.Set("MY_APP_DURATION", "100s")
	envs.Set("MY_APP_DURATION_NULLABLE", "100s")

	configFilePath1 := filesystem.Join(t.TempDir(), "config1.json")
	configFilePath2 := filesystem.Join(t.TempDir(), "config2.json")

	cfg := GenerateAndBindConfig{
		Args: []string{
			"--embedded", "flag takes precedence over ENV",
			"--config-file", configFilePath1,
			"--config-file", configFilePath2,
		},
		EnvNaming:              env.NewNamingConvention("MY_APP_"),
		Envs:                   envs,
		GenerateHelpFlag:       true,
		GenerateConfigFileFlag: true,
		GenerateDumpConfigFlag: true,
	}

	defaultDuration := 123 * time.Second
	defaultAddrValue := netip.AddrFrom4([4]byte{1, 2, 3, 4})
	target := TestConfig{
		Embedded: Embedded{
			EmbeddedField: "default Value",
		},
		SensitiveString: "value1",
		Int:             123,
		Float:           4.56,
		Nested: Nested{
			Foo: "foo",
			Bar: 789,
		},
		StringWithUsage:  "value2",
		Duration:         defaultDuration,
		DurationNullable: &defaultDuration,
		URL:              &url.URL{Scheme: "http", Host: "localhost:1234"},
		Addr:             defaultAddrValue,
		AddrNullable:     &defaultAddrValue,
	}

	// Write YAML config files
	config1 := `
{
  "nested": {
    "foo": "from config",
    "bar": 1000
  },
  "address": "11.22.33.44",
  "addressNullable": "11.22.33.44"
}
`
	require.NoError(t, os.WriteFile(configFilePath1, []byte(config1), 0o600))
	config2 := `
{
  "url": "https://foo.bar",
  "int": 999
}
`
	require.NoError(t, os.WriteFile(configFilePath2, []byte(config2), 0o600))

	// Default values are replaced from the YAML config file.
	require.NoError(t, GenerateAndBind(cfg, &target))
	expectedDuration := 100 * time.Second
	expectedAddrValue := netip.AddrFrom4([4]byte{11, 22, 33, 44})
	assert.Equal(t, TestConfig{
		Embedded: Embedded{
			EmbeddedField: "flag takes precedence over ENV", // from flag
		},
		SensitiveString: "abc",
		Int:             999,
		Float:           78.90,
		Nested: Nested{
			Foo: "from config", // from config
			Bar: 2000,          // from ENV
		},
		StringWithUsage:  "",
		Duration:         expectedDuration,
		DurationNullable: &expectedDuration,
		URL:              &url.URL{Scheme: "https", Host: "foo.bar"},
		Addr:             expectedAddrValue,
		AddrNullable:     &expectedAddrValue,
	}, target)
}

// TestGenerateAndBind_ValueType tests usage of the Value type.
func TestGenerateAndBind_ValueType(t *testing.T) {
	t.Parallel()

	envs := env.Empty()
	envs.Set("MY_APP_EMBEDDED", "foo")
	envs.Set("MY_APP_SENSITIVE_STRING", "abc")
	envs.Set("MY_APP_FLOAT", "78.90")
	envs.Set("MY_APP_NESTED_BAR", "2000")
	envs.Set("MY_APP_STRING_WITH_USAGE", "")
	envs.Set("MY_APP_DURATION", "100s")
	envs.Set("MY_APP_DURATION_NULLABLE", "100s")

	configFilePath := filepath.Join(t.TempDir(), "config.yml") //nolint: forbidigo
	cfg := GenerateAndBindConfig{
		Args: []string{
			"--embedded", "flag takes precedence over ENV",
			"--address", "11.22.33.44",
			"--address-nullable", "11.22.33.44",
			"--config-file", configFilePath,
		},
		EnvNaming:              env.NewNamingConvention("MY_APP_"),
		Envs:                   envs,
		GenerateHelpFlag:       true,
		GenerateConfigFileFlag: true,
		GenerateDumpConfigFlag: true,
	}

	config := `
stringSlice: a,b,c
`
	require.NoError(t, os.WriteFile(configFilePath, []byte(config), 0o600))

	defaultDuration := 123 * time.Second
	defaultAddrValue := netip.AddrFrom4([4]byte{1, 2, 3, 4})
	target := TestConfigWithValueStruct{
		EmbeddedValue: EmbeddedValue{
			EmbeddedField: Value[string]{Value: "default Value"},
		},
		SensitiveString: Value[string]{Value: "value1"},
		Int:             Value[int]{Value: 123},
		Float:           Value[float64]{Value: 4.56},
		Nested: NestedValue{
			Foo: Value[string]{Value: "foo"},
			Bar: Value[int]{Value: 789},
		},
		StringWithUsage:  Value[string]{Value: "value2"},
		Duration:         Value[time.Duration]{Value: defaultDuration},
		DurationNullable: Value[*time.Duration]{Value: &defaultDuration},
		URL:              Value[*url.URL]{Value: &url.URL{Scheme: "http", Host: "localhost:1234"}},
		Addr:             Value[netip.Addr]{Value: defaultAddrValue},
		AddrNullable:     Value[*netip.Addr]{Value: &defaultAddrValue},
	}

	// Default values are replaced from the flags and ENVs
	// SetBy fields are filled in.
	require.NoError(t, GenerateAndBind(cfg, &target))
	expectedDuration := 100 * time.Second
	expectedAddrValue := netip.AddrFrom4([4]byte{11, 22, 33, 44})
	assert.Equal(t, TestConfigWithValueStruct{
		EmbeddedValue: EmbeddedValue{
			EmbeddedField: Value[string]{Value: "flag takes precedence over ENV", SetBy: SetByFlag},
		},
		CustomString:    Value[CustomStringType]{Value: "", SetBy: SetByDefault},
		CustomInt:       Value[CustomIntType]{Value: 0, SetBy: SetByDefault},
		SensitiveString: Value[string]{Value: "abc", SetBy: SetByEnv},
		StringSlice:     Value[[]string]{Value: []string{"a", "b", "c"}, SetBy: SetByConfig},
		Int:             Value[int]{Value: 123, SetBy: SetByDefault},
		IntSlice:        Value[[]int]{Value: nil, SetBy: SetByDefault},
		Float:           Value[float64]{Value: 78.9, SetBy: SetByEnv},
		Nested: NestedValue{
			Foo: Value[string]{Value: "foo", SetBy: SetByDefault},
			Bar: Value[int]{Value: 2000, SetBy: SetByEnv},
		},
		StringWithUsage:  Value[string]{Value: "", SetBy: SetByEnv},
		Duration:         Value[time.Duration]{Value: expectedDuration, SetBy: SetByEnv},
		DurationNullable: Value[*time.Duration]{Value: &expectedDuration, SetBy: SetByEnv},
		URL:              Value[*url.URL]{Value: &url.URL{Scheme: "http", Host: "localhost:1234"}, SetBy: SetByDefault},
		Addr:             Value[netip.Addr]{Value: expectedAddrValue, SetBy: SetByFlag},
		AddrNullable:     Value[*netip.Addr]{Value: &expectedAddrValue, SetBy: SetByFlag},
	}, target)
}

func TestGenerateAndBind_Dump_Yaml(t *testing.T) {
	t.Parallel()

	envs := env.Empty()
	envs.Set("MY_APP_DURATION", "100s")
	envs.Set("MY_APP_DURATION_NULLABLE", "100s")

	cfg := GenerateAndBindConfig{
		Args: []string{
			"app",
			"--dump-config=yaml",
			"--sensitive-string", "abc",
			"--string-with-usage", "invalid",
			"--int", "1000",
			"--float", "78.90",
		},
		EnvNaming:              env.NewNamingConvention("MY_APP_"),
		Envs:                   envs,
		GenerateHelpFlag:       true,
		GenerateConfigFileFlag: true,
		GenerateDumpConfigFlag: true,
	}

	target := TestConfig{
		Embedded: Embedded{
			EmbeddedField: "default",
		},
	}

	expected := `
embedded: default
customString: ""
customInt: 0
sensitiveString: '*****'
stringSlice: []
int: 1000
intSlice: []
float: 78.9
# An usage text. Validation rules: ne=invalid
stringWithUsage: invalid
duration: 1m40s
durationNullable: 1m40s
url: null
address: ""
addressNullable: null
nested:
    foo: ""
    bar: 0
`

	err := GenerateAndBind(cfg, &target)
	if assert.Error(t, err) {
		dumpErr, ok := err.(DumpError)
		require.True(t, ok)
		assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(string(dumpErr.Dump)))
		assert.Equal(t, "configuration dump requested", dumpErr.Error())
		if err := dumpErr.ValidationError; assert.Error(t, err) {
			assert.Equal(t, "configuration is not valid:\n- \"stringWithUsage\" should not be equal to invalid", err.Error())
		}
	}
}
