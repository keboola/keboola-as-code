package replacevalues

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/deepcopy"
)

type Values struct {
	values    []Value
	validated bool
}

// Value to be replaced.
type Value struct {
	Old interface{}
	New interface{}
}

// SubString represents partial match in a string.
type SubString string

func NewValues() *Values {
	return &Values{}
}

func (v *Values) Values() []Value {
	out := make([]Value, len(v.values))
	copy(out, v.values)
	return out
}

func (v *Values) AddValue(oldValue, newValue interface{}) {
	v.values = append(v.values, Value{Old: oldValue, New: newValue})
}

func (v *Values) AddKey(oldKey, newKey model.Key) {
	switch oldKey := oldKey.(type) {
	case model.BranchKey:
		v.AddValue(oldKey, newKey.(model.BranchKey))
		v.AddId(oldKey.Id, newKey.(model.BranchKey).Id)
	case model.ConfigKey:
		v.AddValue(oldKey, newKey.(model.ConfigKey))
		v.AddId(oldKey.Id, newKey.(model.ConfigKey).Id)
	case model.ConfigRowKey:
		v.AddValue(oldKey, newKey.(model.ConfigRowKey))
		v.AddId(oldKey.Id, newKey.(model.ConfigRowKey).Id)
	default:
		panic(fmt.Errorf(`unexpected key type "%T"`, oldKey))
	}
}

func (v *Values) AddId(oldId, newId interface{}) {
	switch old := oldId.(type) {
	case model.BranchId:
		v.AddValue(old, newId.(model.BranchId))
	case model.ConfigId:
		v.AddValue(old, newId.(model.ConfigId))
		// ConfigId in strings
		v.AddValue(SubString(old), string(newId.(model.ConfigId)))
	case model.RowId:
		v.AddValue(old, newId.(model.RowId))
		// ConfigRowId in strings
		v.AddValue(SubString(old), string(newId.(model.RowId)))
	default:
		panic(fmt.Errorf(`unexpected ID type "%T"`, old))
	}
}

func (v *Values) Replace(input interface{}) (interface{}, error) {
	if err := v.validate(); err != nil {
		return nil, err
	}

	return deepcopy.CopyTranslate(input, func(original, clone reflect.Value, steps deepcopy.Steps) {
		for _, item := range v.values {
			switch v := item.Old.(type) {
			case SubString:
				// Search and replace sub-string
				if original.IsValid() && original.Type().String() == "string" {
					if modified, found := v.replace(original.String(), item.New.(string)); found {
						clone.Set(reflect.ValueOf(modified))
					}
				}
			default:
				// Replace other types
				if original.IsValid() && original.Interface() == item.Old {
					clone.Set(reflect.ValueOf(item.New))
				}
			}
		}
	}), nil
}

// validate - old and new IDs must be unique.
func (v *Values) validate() error {
	// Only once
	if v.validated {
		return nil
	}

	// Old IDs must be unique
	valueByString := make(map[interface{}][]interface{})
	for _, item := range v.values {
		valueByString[item.Old] = append(valueByString[item.Old], item.Old)
	}
	for k, v := range valueByString {
		if len(v) > 1 {
			return fmt.Errorf(`the old ID "%s" is defined %dx`, k, len(v))
		}
	}

	// New IDs must be unique
	valueByString = make(map[interface{}][]interface{})
	for _, item := range v.values {
		valueByString[item.New] = append(valueByString[item.New], item.New)
	}
	for k, v := range valueByString {
		if len(v) > 1 {
			return fmt.Errorf(`the new ID "%s" is defined %dx`, k, len(v))
		}
	}

	v.validated = true
	return nil
}

func (s SubString) replace(full, replacement string) (string, bool) {
	re := regexpcache.MustCompile(fmt.Sprintf(
		`(^|[^a-zA-Z0-9])(` + // $1: start OR not alphanum
			regexp.QuoteMeta(string(s)) + // $2: searched sub-string
			`)($|[^a-zA-Z0-9])`, // $3: end OR not alphanum
	))
	if re.MatchString(full) {
		replacement = strings.ReplaceAll(replacement, `$`, `$$`)
		return re.ReplaceAllString(full, `${1}`+replacement+`${3}`), true
	}
	return "", false
}
