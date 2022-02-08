package input

// Options for input KindSelect and KindMultiSelect.
// - KindSelect: user can select one value.
// - KindMultiSelect: user can select multiple values.
type Options []Option

type Option struct {
	Id   string `json:"id" validate:"required"`
	Name string `json:"name" validate:"required"`
}

func (options Options) GetById(id string) (Option, int, bool) {
	for i, o := range options {
		if o.Id == id {
			return o, i, true
		}
	}
	return Option{}, -1, false
}

func (options Options) ContainsId(id string) bool {
	_, _, found := options.GetById(id)
	return found
}

func (options Options) Names() []string {
	out := make([]string, 0)
	for _, o := range options {
		out = append(out, o.Name)
	}
	return out
}

// validateDefaultOptions - default options must be present in the input allowed Options.
func validateDefaultOptions(value interface{}, kind Kind, options Options) bool {
	// Default options must be present in the input allowed Options.
	switch kind {
	case KindSelect:
		if v, ok := value.(string); ok {
			return options.ContainsId(v)
		} else {
			// Unexpected type, it is validated by a separate rule
			return true
		}
	case KindMultiSelect:
		if values, ok := value.([]interface{}); ok {
			for _, value := range values {
				if valueStr, ok := value.(string); !ok || !options.ContainsId(valueStr) {
					// Invalid type or not found
					return false
				}
			}
		}
		return true
	}

	return true
}
