package configmap

import (
	"reflect"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type VisitStruct struct {
	String  Value[CustomStringType]
	Int     Value[CustomIntType]
	Nested1 Nested
	Nested2 *Nested
}

func TestVisit(t *testing.T) {
	t.Parallel()

	var keys []string
	err := Visit(reflect.ValueOf(VisitStruct{}), VisitConfig{
		OnField: func(field reflect.StructField, path orderedmap.Path) (fieldName string, ok bool) {
			return field.Name, true
		},
		OnValue: func(vc *VisitContext) error {
			keys = append(keys, vc.MappedPath.String())
			return nil
		},
	})

	require.NoError(t, err)
	assert.Equal(t, []string{
		"",
		"String",
		"String.Value",
		"String.SetBy",
		"Int",
		"Int.Value",
		"Int.SetBy",
		"Nested1",
		"Nested1.Ignored",
		"Nested1.Foo",
		"Nested1.Bar",
		// Visit function visits also empty pointers, see Nested2 field
		"Nested2",
		"Nested2.Ignored",
		"Nested2.Foo",
		"Nested2.Bar",
	}, keys)
}
