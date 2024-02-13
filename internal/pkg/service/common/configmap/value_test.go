package configmap_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
)

func TestValue_IsSet(t *testing.T) {
	t.Parallel()

	v1 := configmap.Value[string]{Value: "foo", SetBy: configmap.SetByDefault}
	assert.False(t, v1.IsSet())

	v2 := configmap.Value[string]{Value: "foo", SetBy: configmap.SetByFlag}
	assert.True(t, v2.IsSet())
}

func TestNewValue(t *testing.T) {
	t.Parallel()
	assert.Equal(t, configmap.Value[int]{Value: 123, SetBy: configmap.SetByDefault}, configmap.NewValue(123))
}
