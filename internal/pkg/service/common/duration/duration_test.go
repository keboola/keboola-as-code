package duration_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
)

type Data struct {
	Duration duration.Duration `json:"duration" yaml:"duration"`
}

func TestDuration_MarshalText_JSON(t *testing.T) {
	t.Parallel()
	data := Data{Duration: duration.From(123 * time.Hour)}
	jsonOut, err := json.Marshal(data)
	require.NoError(t, err)
	assert.JSONEq(t, `{"duration":"123h0m0s"}`, string(jsonOut))
}

func TestDuration_MarshalText_YAML(t *testing.T) {
	t.Parallel()
	data := Data{Duration: duration.From(123 * time.Hour)}
	yamlOut, err := yaml.Marshal(data)
	require.NoError(t, err)
	assert.YAMLEq(t, "duration: 123h0m0s\n", string(yamlOut))
}

func TestDuration_UnmarshalText_JSON(t *testing.T) {
	t.Parallel()
	var data Data
	require.NoError(t, json.Unmarshal([]byte(`{"duration":"123h0m0s"}`), &data))
	assert.Equal(t, "123h0m0s", data.Duration.String())
}

func TestDuration_UnmarshalText_YAML(t *testing.T) {
	t.Parallel()
	var data Data
	require.NoError(t, yaml.Unmarshal([]byte("duration: 123h0m0s\n"), &data))
	assert.Equal(t, "123h0m0s", data.Duration.String())
}

func TestDuration_UnmarshalInt_JSON(t *testing.T) {
	t.Parallel()
	var data Data
	require.NoError(t, json.Unmarshal([]byte(`{"duration":456}`), &data))
	assert.Equal(t, "456ns", data.Duration.String())
}

func TestDuration_UnmarshalInt_YAML(t *testing.T) {
	t.Parallel()
	var data Data
	require.NoError(t, yaml.Unmarshal([]byte("duration: 456\n"), &data))
	assert.Equal(t, "456ns", data.Duration.String())
}
