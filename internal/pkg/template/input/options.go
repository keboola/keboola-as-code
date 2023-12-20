package input

import (
	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Options for input KindSelect and KindMultiSelect.
// - KindSelect: user can select one value.
// - KindMultiSelect: user can select multiple values.
type Options []Option

type Option struct {
	Value string `json:"value" validate:"required,max=100"`
	Label string `json:"label" validate:"required,min=1,max=25"`
}

func (options Options) GetByID(id string) (Option, int, bool) {
	for i, o := range options {
		if o.Value == id {
			return o, i, true
		}
	}
	return Option{}, -1, false
}

func (options Options) ContainsID(id string) bool {
	_, _, found := options.GetByID(id)
	return found
}

func (options Options) Names() []string {
	out := make([]string, 0)
	for _, o := range options {
		out = append(out, o.Label)
	}
	return out
}

// Map returns id -> name ordered map.
func (options Options) Map() *orderedmap.OrderedMap {
	out := orderedmap.New()
	for _, o := range options {
		out.Set(o.Value, o.Label)
	}
	return out
}

func OptionsFromString(str string) (out Options, err error) {
	if str == "" {
		return nil, nil
	}
	pairs := orderedmap.New()
	if err := json.DecodeString(str, pairs); err != nil {
		return nil, errors.Errorf(`value "%s" is not valid: %w`, str, err)
	}

	for _, key := range pairs.Keys() {
		valueRaw, _ := pairs.Get(key)
		if v, ok := valueRaw.(string); ok {
			out = append(out, Option{Value: key, Label: v})
		} else {
			return nil, errors.Errorf(`value "%s" is not valid: value of key "%s" must be string`, str, key)
		}
	}

	return out, nil
}

// validateDefaultOptions - default options must be present in the input allowed Options.
func validateDefaultOptions(value any, kind Kind, options Options) bool {
	// Default options must be present in the input allowed Options.
	switch kind {
	case KindSelect:
		if v, ok := value.(string); ok {
			return options.ContainsID(v)
		} else {
			// Unexpected type, it is validated by a separate rule
			return true
		}
	case KindMultiSelect:
		if values, ok := value.([]any); ok {
			for _, value := range values {
				if valueStr, ok := value.(string); !ok || !options.ContainsID(valueStr) {
					// Invalid type or not found
					return false
				}
			}
		}
		return true
	default:
		return true
	}
}
