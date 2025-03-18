package serde

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
)

func TestSerde_JSON_Decode(t *testing.T) {
	t.Parallel()

	serde := NewJSON(NoValidation)
	value := `{"someKey":{"key":1}}`
	kv := &op.KeyValue{Value: []byte(value)}
	{
		var decoded map[string]struct {
			Key float64 `json:"key"`
		}

		err := serde.Decode(t.Context(), kv, &decoded)
		require.NoError(t, err)
		expected := map[string]struct {
			Key float64 `json:"key"`
		}{"someKey": {Key: 1.0}}
		assert.Equal(t, expected, decoded)
	}

	{
		var decoded map[string]any

		err := serde.Decode(t.Context(), kv, &decoded)
		require.NoError(t, err)
		expected := map[string]any{"someKey": map[string]any{"key": 1.0}}

		assert.Equal(t, expected, decoded)

		// Assert exact types using reflect
		someKeyValue := decoded["someKey"]
		assert.Equal(t, "map[string]interface {}", reflect.TypeOf(someKeyValue).String())

		innerMap := someKeyValue.(map[string]any)
		keyValue := innerMap["key"]
		assert.Equal(t, "float64", reflect.TypeOf(keyValue).String())
	}
}

func TestSerde_JSONWithNumbers_Decode(t *testing.T) {
	t.Parallel()

	serde := NewJSONWithNumbers(NoValidation)
	value := `{"someKey":{"key":1}}`
	kv := &op.KeyValue{Value: []byte(value)}
	{
		var decoded map[string]struct {
			Key float64 `json:"key"`
		}

		err := serde.Decode(t.Context(), kv, &decoded)
		require.NoError(t, err)
		expected := map[string]struct {
			Key float64 `json:"key"`
		}{"someKey": {Key: 1.0}}
		assert.Equal(t, expected, decoded)
	}

	{
		var decoded map[string]any

		err := serde.Decode(t.Context(), kv, &decoded)
		require.NoError(t, err)
		expected := map[string]any{"someKey": map[string]any{"key": json.Number("1")}}

		assert.Equal(t, expected, decoded)

		// Assert exact types using reflect
		someKeyValue := decoded["someKey"]
		assert.Equal(t, "map[string]interface {}", reflect.TypeOf(someKeyValue).String())

		innerMap := someKeyValue.(map[string]any)
		keyValue := innerMap["key"]
		assert.Equal(t, "json.Number", reflect.TypeOf(keyValue).String())
	}
}
