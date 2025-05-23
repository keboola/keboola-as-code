// Package function contains definitions of Jsonnet functions for various template contexts (use, upgrade, ...).
// Not all functions are available in all template contexts.
//
// The following applies to each function:
//   - Parameters of the Go function are external dependencies of the Jsonnet function.
//   - Result of the Go function is definition of the Jsonnet function.
package function

import (
	"slices"

	"github.com/google/go-jsonnet/ast"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

const (
	SnowflakeWriterIDAws   = keboola.ComponentID("keboola.wr-db-snowflake")
	SnowflakeWriterIDAzure = keboola.ComponentID("keboola.wr-snowflake-blob-storage")
	SnowflakeWriterIDGCP   = keboola.ComponentID("keboola.wr-db-snowflake-gcs")
	SnowflakeWriterIDGCPS3 = keboola.ComponentID("keboola.wr-db-snowflake-gcs-s3")
)

// ConfigID Jsonnet function maps configuration ID used in the template
// to configuration ID used in the project.
func ConfigID(idMapper func(id any) string) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name:   `ConfigId`,
		Params: ast.Identifiers{"id"},
		Func: func(params []any) (any, error) {
			if len(params) != 1 {
				return nil, errors.Errorf("one parameter expected, found %d", len(params))
			} else if id, ok := params[0].(string); !ok {
				return nil, errors.New("parameter must be a string")
			} else {
				return idMapper(keboola.ConfigID(id)), nil
			}
		},
	}
}

// ConfigRowID Jsonnet function maps configuration row ID used in the template
// to configuration ID used in the project.
func ConfigRowID(idMapper func(id any) string) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name:   `ConfigRowId`,
		Params: ast.Identifiers{"id"},
		Func: func(params []any) (any, error) {
			if len(params) != 1 {
				return nil, errors.Errorf("one parameter expected, found %d", len(params))
			} else if id, ok := params[0].(string); !ok {
				return nil, errors.New("parameter must be a string")
			} else {
				return idMapper(keboola.RowID(id)), nil
			}
		},
	}
}

// Input Jsonnet function returns input value.
func Input(inputValueProvider func(inputID string) (input.Value, bool)) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name:   `Input`,
		Params: ast.Identifiers{"id"},
		Func: func(params []any) (any, error) {
			if len(params) != 1 {
				return nil, errors.Errorf("one parameter expected, found %d", len(params))
			} else if id, ok := params[0].(string); !ok {
				return nil, errors.New("parameter must be a string")
			} else if v, found := inputValueProvider(id); !found {
				return nil, errors.Errorf(`input "%s" not found`, id)
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
func InputIsAvailable(inputValueProvider func(inputID string) (input.Value, bool)) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name:   `InputIsAvailable`,
		Params: ast.Identifiers{"id"},
		Func: func(params []any) (any, error) {
			if len(params) != 1 {
				return nil, errors.Errorf("one parameter expected, found %d", len(params))
			} else if id, ok := params[0].(string); !ok {
				return nil, errors.New("parameter must be a string")
			} else if v, found := inputValueProvider(id); !found {
				return nil, errors.Errorf(`input "%s" not found`, id)
			} else {
				return !v.Skipped, nil
			}
		},
	}
}

// InstanceID Jsonnet function returns full id of the template instance.
func InstanceID(instanceID string) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name:   `InstanceId`,
		Params: ast.Identifiers{},
		Func: func(params []any) (any, error) {
			return instanceID, nil
		},
	}
}

// InstanceIDShort Jsonnet function returns shortened id of the template instance.
func InstanceIDShort(instanceIDShort string) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name:   `InstanceIdShort`,
		Params: ast.Identifiers{},
		Func: func(params []any) (any, error) {
			return instanceIDShort, nil
		},
	}
}

// RandomID Jsonnet function returns a random, shortened id of the template instance.
func RandomID() *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name:   `RandomID`,
		Params: ast.Identifiers{},
		Func: func(params []any) (any, error) {
			instanceID := idgenerator.TemplateInstanceID()
			return strhelper.FirstN(instanceID, 8), nil
		},
	}
}

// ComponentIsAvailable Jsonnet function returns true if the component is available in the stack.
func ComponentIsAvailable(components *model.ComponentsMap) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name:   `ComponentIsAvailable`,
		Params: ast.Identifiers{"componentId"},
		Func: func(params []any) (any, error) {
			if len(params) != 1 {
				return nil, errors.Errorf("one parameter expected, found %d", len(params))
			} else if componentID, ok := params[0].(string); !ok {
				return nil, errors.New("parameter must be a string")
			} else {
				_, found := components.Get(keboola.ComponentID(componentID))
				return found, nil
			}
		},
	}
}

// SnowflakeWriterComponentID Jsonnet function returns component ID of the Snowflake Writer it the stack.
func SnowflakeWriterComponentID(components *model.ComponentsMap, backends []string) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name:   `SnowflakeWriterComponentId`,
		Params: ast.Identifiers{},
		Func: func(params []any) (any, error) {
			switch {
			case components.Has(SnowflakeWriterIDAws):
				return SnowflakeWriterIDAws.String(), nil
			case components.Has(SnowflakeWriterIDAzure):
				return SnowflakeWriterIDAzure.String(), nil
			case components.Has(SnowflakeWriterIDGCPS3) && slices.Contains(backends, project.BackendSnowflake):
				return SnowflakeWriterIDGCPS3.String(), nil
			case components.Has(SnowflakeWriterIDGCP) && slices.Contains(backends, project.BackendBigQuery):
				return SnowflakeWriterIDGCP.String(), nil
			default:
				return nil, errors.New("no Snowflake Writer component found")
			}
		},
	}
}

// HasProjectBackend Jsonnet function returns true if the project backend is available, otherwise false.
func HasProjectBackend(backends []string) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name:   `HasProjectBackend`,
		Params: ast.Identifiers{"backend"},
		Func: func(params []any) (any, error) {
			if len(params) != 1 {
				return nil, errors.Errorf("one parameter expected, found %d", len(params))
			}

			for _, backend := range backends {
				if backend == params[0] {
					return true, nil
				}
			}
			return false, nil
		},
	}
}
