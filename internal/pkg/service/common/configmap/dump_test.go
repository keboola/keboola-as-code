package configmap

import (
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDumpFlat_Empty(t *testing.T) {
	t.Parallel()
	bytes, err := NewDumper().Dump(TestConfig{}).Flat().AsJSON(true)
	require.NoError(t, err)
	assert.JSONEq(t, strings.TrimSpace(`
{
  "address": "",
  "addressNullable": null,
  "byteSlice": "",
  "customInt": 0,
  "customString": "",
  "duration": "0s",
  "durationNullable": null,
  "embedded": "",
  "float": 0,
  "int": 0,
  "intSlice": [],
  "nested.bar": 0,
  "nested.foo": "",
  "sensitiveString": "*****",
  "stringSlice": [],
  "stringWithUsage": "",
  "url": null
}
`), strings.TrimSpace(string(bytes)))
}

func TestDumpFlat(t *testing.T) {
	t.Parallel()
	bytes, err := NewDumper().Dump(dumpTestConfig()).Flat().AsJSON(true)
	require.NoError(t, err)
	assert.JSONEq(t, strings.TrimSpace(`
{
  "address": "1.2.3.4",
  "addressNullable": null,
  "byteSlice": "dmFsdWUz",
  "customInt": 567,
  "customString": "custom",
  "duration": "2m3s",
  "durationNullable": "2m3s",
  "embedded": "embedded Value",
  "float": 4.56,
  "int": 123,
  "intSlice": [
    10,
    20
  ],
  "nested.bar": 789,
  "nested.foo": "foo",
  "sensitiveString": "*****",
  "stringSlice": [
    "foo",
    "bar"
  ],
  "stringWithUsage": "value2",
  "url": "http://localhost:1234"
}
`), strings.TrimSpace(string(bytes)))
}

func TestDumpAsJSON(t *testing.T) {
	t.Parallel()
	bytes, err := NewDumper().Dump(dumpTestConfig()).AsJSON(true)
	require.NoError(t, err)
	assert.JSONEq(t, strings.TrimSpace(`
{
  "embedded": "embedded Value",
  "customString": "custom",
  "customInt": 567,
  "sensitiveString": "*****",
  "stringSlice": [
    "foo",
    "bar"
  ],
  "int": 123,
  "intSlice": [
    10,
    20
  ],
  "float": 4.56,
  "stringWithUsage": "value2",
  "duration": "2m3s",
  "durationNullable": "2m3s",
  "url": "http://localhost:1234",
  "address": "1.2.3.4",
  "addressNullable": null,
  "byteSlice": "dmFsdWUz",
  "nested": {
    "foo": "foo",
    "bar": 789
  }
}
`), strings.TrimSpace(string(bytes)))
}

func TestDumpAsYAML(t *testing.T) {
	t.Parallel()
	bytes, err := NewDumper().Dump(dumpTestConfig()).AsYAML()
	require.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(`
embedded: embedded Value
customString: custom
customInt: 567
sensitiveString: '*****'
stringSlice:
    - foo
    - bar
int: 123
intSlice:
    - 10
    - 20
float: 4.56
# An usage text. Validation rules: ne=invalid
stringWithUsage: value2
duration: 2m3s
durationNullable: 2m3s
url: http://localhost:1234
address: 1.2.3.4
addressNullable: null
byteSlice: dmFsdWUz
nested:
    foo: foo
    bar: 789
`), strings.TrimSpace(string(bytes)))
}

// TestDump_ValueStruct_Empty tests dumping of a configuration structure with empty Value fields.
func TestDump_ValueStruct_Empty(t *testing.T) {
	t.Parallel()
	bytes, err := NewDumper().Dump(TestConfigWithValueStruct{}).AsYAML()
	require.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(`
embedded: ""
customString: ""
customInt: 0
sensitiveString: '*****'
stringSlice: []
int: 0
intSlice: []
float: 0
# An usage text.
stringWithUsage: ""
duration: 0s
durationNullable: null
url: null
address: ""
addressNullable: null
byteSlice: ""
nested:
    foo: ""
    bar: 0
`), strings.TrimSpace(string(bytes)))
}

// TestDump_ValueStruct tests dumping of a configuration structure with Value fields.
func TestDump_ValueStruct(t *testing.T) {
	t.Parallel()
	bytes, err := NewDumper().Dump(dumpTestConfigWithValueStruct()).AsYAML()
	require.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(`
embedded: embedded Value
customString: custom
customInt: 567
sensitiveString: '*****'
stringSlice:
    - foo
    - bar
int: 123
intSlice:
    - 10
    - 20
float: 4.56
# An usage text.
stringWithUsage: value2
duration: 2m3s
durationNullable: 2m3s
url: http://localhost:1234
address: 1.2.3.4
addressNullable: null
byteSlice: dmFsdWUz
nested:
    foo: foo
    bar: 789
`), strings.TrimSpace(string(bytes)))
}

func dumpTestConfig() TestConfig {
	duration, _ := time.ParseDuration("123s")
	addrValue := netip.AddrFrom4([4]byte{1, 2, 3, 4})
	return TestConfig{
		Embedded:         Embedded{EmbeddedField: "embedded Value"},
		CustomString:     "custom",
		CustomInt:        567,
		SensitiveString:  "password",
		StringSlice:      []string{"foo", "bar"},
		Int:              123,
		IntSlice:         []int{10, 20},
		Float:            4.56,
		StringWithUsage:  "value2",
		Duration:         duration,
		DurationNullable: &duration,
		URL:              &url.URL{Scheme: "http", Host: "localhost:1234"},
		Addr:             addrValue,
		ByteSlice:        []byte("value3"),
		Nested: Nested{
			Foo: "foo",
			Bar: 789,
		},
		Skipped: true,
	}
}

func dumpTestConfigWithValueStruct() TestConfigWithValueStruct {
	duration, _ := time.ParseDuration("123s")
	addrValue := netip.AddrFrom4([4]byte{1, 2, 3, 4})
	return TestConfigWithValueStruct{
		EmbeddedValue: EmbeddedValue{
			EmbeddedField: Value[string]{Value: "embedded Value"},
		},
		CustomString:     Value[CustomStringType]{Value: "custom"},
		CustomInt:        Value[CustomIntType]{Value: 567},
		SensitiveString:  Value[string]{Value: "password"},
		StringSlice:      Value[[]string]{Value: []string{"foo", "bar"}},
		Int:              Value[int]{Value: 123},
		IntSlice:         Value[[]int]{Value: []int{10, 20}},
		Float:            Value[float64]{Value: 4.56},
		StringWithUsage:  Value[string]{Value: "value2"},
		Duration:         Value[time.Duration]{Value: duration},
		DurationNullable: Value[*time.Duration]{Value: &duration},
		URL:              Value[*url.URL]{Value: &url.URL{Scheme: "http", Host: "localhost:1234"}},
		Addr:             Value[netip.Addr]{Value: addrValue},
		ByteSlice:        Value[[]byte]{Value: []byte("value3")},
		Nested: NestedValue{
			Foo: Value[string]{Value: "foo"},
			Bar: Value[int]{Value: 789},
		},
		Skipped: true,
	}
}
