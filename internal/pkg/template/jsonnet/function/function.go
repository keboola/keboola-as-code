// Package function contains definitions of Jsonnet functions for various template contexts (use, upgrade, ...).
// Not all functions are available in all template contexts.
//
// The following applies to each function:
//   - Parameters of the Go function are external dependencies of the Jsonnet function.
//   - Result of the Go function is definition of the Jsonnet function.
package function

import (
	"fmt"

	"github.com/google/go-jsonnet/ast"
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
)

const (
	SnowflakeWriterIDAws   = storageapi.ComponentID("keboola.wr-db-snowflake")
	SnowflakeWriterIDAzure = storageapi.ComponentID("keboola.wr-snowflake-blob-storage")
)

// ConfigId Jsonnet function maps configuration ID used in the template
// to configuration ID used in the project.
func ConfigId(idMapper func(id interface{}) string) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name:   `ConfigId`,
		Params: ast.Identifiers{"id"},
		Func: func(params []interface{}) (interface{}, error) {
			if len(params) != 1 {
				return nil, fmt.Errorf("one parameter expected, found %d", len(params))
			} else if id, ok := params[0].(string); !ok {
				return nil, fmt.Errorf("parameter must be a string")
			} else {
				return idMapper(storageapi.ConfigID(id)), nil
			}
		},
	}
}

// ConfigRowId Jsonnet function maps configuration row ID used in the template
// to configuration ID used in the project.
func ConfigRowId(idMapper func(id interface{}) string) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name:   `ConfigRowId`,
		Params: ast.Identifiers{"id"},
		Func: func(params []interface{}) (interface{}, error) {
			if len(params) != 1 {
				return nil, fmt.Errorf("one parameter expected, found %d", len(params))
			} else if id, ok := params[0].(string); !ok {
				return nil, fmt.Errorf("parameter must be a string")
			} else {
				return idMapper(storageapi.RowID(id)), nil
			}
		},
	}
}

// Input Jsonnet function returns input value.
func Input(inputValueProvider func(inputId string) (input.Value, bool)) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name:   `Input`,
		Params: ast.Identifiers{"id"},
		Func: func(params []interface{}) (interface{}, error) {
			if len(params) != 1 {
				return nil, fmt.Errorf("one parameter expected, found %d", len(params))
			} else if id, ok := params[0].(string); !ok {
				return nil, fmt.Errorf("parameter must be a string")
			} else if v, found := inputValueProvider(id); !found {
				return nil, fmt.Errorf(`input "%s" not found`, id)
			} else {
				switch v := v.Value.(type) {
				case int:
					return float64(v), nil
				default:
					return v, nil
				}
			}
		},
	}
}

// InputIsAvailable Jsonnet function returns true if the input exists.
func InputIsAvailable(inputValueProvider func(inputId string) (input.Value, bool)) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name:   `InputIsAvailable`,
		Params: ast.Identifiers{"id"},
		Func: func(params []interface{}) (interface{}, error) {
			if len(params) != 1 {
				return nil, fmt.Errorf("one parameter expected, found %d", len(params))
			} else if id, ok := params[0].(string); !ok {
				return nil, fmt.Errorf("parameter must be a string")
			} else if v, found := inputValueProvider(id); !found {
				return nil, fmt.Errorf(`input "%s" not found`, id)
			} else {
				return !v.Skipped, nil
			}
		},
	}
}

// InstanceId Jsonnet function returns full id of the template instance.
func InstanceId(instanceId string) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name:   `InstanceId`,
		Params: ast.Identifiers{},
		Func: func(params []interface{}) (interface{}, error) {
			return instanceId, nil
		},
	}
}

// InstanceIdShort Jsonnet function returns shortened id of the template instance.
func InstanceIdShort(instanceIdShort string) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name:   `InstanceIdShort`,
		Params: ast.Identifiers{},
		Func: func(params []interface{}) (interface{}, error) {
			return instanceIdShort, nil
		},
	}
}

// ComponentIsAvailable Jsonnet function returns true if the component is available in the stack.
func ComponentIsAvailable(components *model.ComponentsMap) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name:   `ComponentIsAvailable`,
		Params: ast.Identifiers{"componentId"},
		Func: func(params []interface{}) (interface{}, error) {
			if len(params) != 1 {
				return nil, fmt.Errorf("one parameter expected, found %d", len(params))
			} else if componentId, ok := params[0].(string); !ok {
				return nil, fmt.Errorf("parameter must be a string")
			} else {
				_, found := components.Get(storageapi.ComponentID(componentId))
				return found, nil
			}
		},
	}
}

// SnowflakeWriterComponentId Jsonnet function returns component ID of the Snowflake Writer it the stack.
func SnowflakeWriterComponentId(components *model.ComponentsMap) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name:   `SnowflakeWriterComponentId`,
		Params: ast.Identifiers{},
		Func: func(params []interface{}) (interface{}, error) {
			if _, found := components.Get(SnowflakeWriterIDAws); found {
				return SnowflakeWriterIDAws.String(), nil
			} else if _, found := components.Get(SnowflakeWriterIDAzure); found {
				return SnowflakeWriterIDAzure.String(), nil
			} else {
				return nil, fmt.Errorf("no Snowflake Writer component found")
			}
		},
	}
}
