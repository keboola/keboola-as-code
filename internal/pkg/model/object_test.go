package model

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestBranch_Clone(t *testing.T) {
	t.Parallel()
	value := &Branch{
		BranchKey:   BranchKey{Id: 123},
		Name:        "foo",
		Description: "bar",
		IsDefault:   true,
	}
	assertDeepEqualNotSame(t, value, value.Clone(), "")
}

func TestConfig_Clone(t *testing.T) {
	t.Parallel()
	value := &Config{
		ConfigKey:         ConfigKey{BranchId: 123, ComponentId: `foo.bar`, Id: `456`},
		Name:              "foo",
		Description:       "bar",
		ChangeDescription: `my change`,
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{Key: "key", Value: "value"},
		}),
		Transformation: &Transformation{
			Blocks: []*Block{
				{
					BlockKey: BlockKey{
						BranchId:    123,
						ComponentId: `foo.bar`,
						ConfigId:    `456`,
						Index:       1,
					},
					Name: "my block",
					Codes: Codes{
						{
							CodeKey: CodeKey{
								BranchId:    123,
								ComponentId: `foo.bar`,
								ConfigId:    `456`,
								BlockIndex:  1,
								Index:       1,
							},
							Name:    "my code",
							Scripts: []string{"foo", "bar"},
						},
					},
				},
			},
			LinkToSharedCode: &LinkToSharedCode{
				Config: ConfigKey{
					BranchId:    123,
					ComponentId: SharedCodeComponentId,
					Id:          `456`,
				},
				Rows: UsedSharedCodeRows{
					ConfigRowKey{
						BranchId:    123,
						ComponentId: SharedCodeComponentId,
						ConfigId:    `456`,
						Id:          `789`,
					},
				},
			},
		},
		Orchestration: &Orchestration{
			Phases: []*Phase{
				{
					Name:    `foo`,
					Content: orderedmap.New(),
					Tasks: []*Task{
						{
							Name:    `bar`,
							Content: orderedmap.New(),
						},
					},
				},
			},
		},
		Relations: Relations{
			&VariablesForRelation{
				ComponentId: `foo.bar`,
				ConfigId:    `789`,
			},
		},
	}
	assertDeepEqualNotSame(t, value, value.Clone(), "")
}

func TestConfigRow_Clone(t *testing.T) {
	t.Parallel()
	value := &ConfigRow{
		ConfigRowKey:      ConfigRowKey{BranchId: 123, ComponentId: `foo.bar`, ConfigId: `456`, Id: `789`},
		Name:              "foo",
		Description:       "bar",
		ChangeDescription: `my change`,
		IsDisabled:        true,
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{Key: "key", Value: "value"},
		}),
	}
	assertDeepEqualNotSame(t, value, value.Clone(), "")
}

func assertDeepEqualNotSame(t *testing.T, a, b interface{}, path string) {
	t.Helper()

	// Equal
	assert.Equal(t, a, b, path)

	// Same type
	typeA := reflect.TypeOf(a)
	typeB := reflect.TypeOf(b)
	if typeA.String() != typeB.String() {
		assert.FailNowf(t, `different types`, `A (%s) and B (%s) have different types`, typeA.String(), typeB.String())
	}

	// But not same (points to different values)
	assert.NotSamef(t, a, b, `%s, path: %s`, typeA.String(), path)

	// Nested fields
	valueA := reflect.ValueOf(a)
	valueB := reflect.ValueOf(b)
	if typeA.Kind() == reflect.Ptr {
		typeA = typeA.Elem()
		valueA = valueA.Elem()
		valueB = valueB.Elem()
	}
	switch typeA.Kind() {
	case reflect.Struct:
		for i := 0; i < typeA.NumField(); i++ {
			field := typeA.Field(i)
			fieldA := valueA.Field(i)
			fieldB := valueB.Field(i)
			if !fieldA.CanAddr() {
				continue
			} else if !fieldA.CanInterface() {
				// Read unexported fields
				fieldA = reflect.NewAt(field.Type, unsafe.Pointer(fieldA.UnsafeAddr())).Elem()
				fieldB = reflect.NewAt(field.Type, unsafe.Pointer(fieldB.UnsafeAddr())).Elem()
			}

			assertDeepEqualNotSame(
				t,
				fieldA.Interface(),
				fieldB.Interface(),
				path+`.`+field.Name,
			)
		}
	case reflect.Slice:
		for i := 0; i < valueA.Len(); i++ {
			assertDeepEqualNotSame(
				t,
				valueA.Index(i).Interface(),
				valueB.Index(i).Interface(),
				path+`.`+cast.ToString(i),
			)

			// Underlying array must be different, check address of the value
			assert.NotSame(t, valueA.Index(i).Addr().Interface(), valueB.Index(i).Addr().Interface(), path+`.`+cast.ToString(i))
		}
	case reflect.Map:
		for _, k := range valueA.MapKeys() {
			assertDeepEqualNotSame(
				t,
				valueA.MapIndex(k).Interface(),
				valueB.MapIndex(k).Interface(),
				path+`.`+cast.ToString(k.Interface()),
			)
		}
	}
}
