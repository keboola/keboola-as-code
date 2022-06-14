package replacevalues

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/keboola/go-utils/pkg/deepcopy"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/cast"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type Values struct {
	values    []Value
	validated bool
}

// Value to be replaced.
type Value struct {
	Search  interface{}
	Replace interface{}
}

// SubString represents partial match in a string.
type SubString string

// ContentField sets nested value in config/row.Content ordered map.
type ContentField struct {
	objectKey model.Key
	fieldPath orderedmap.Path
}

func NewValues() *Values {
	return &Values{}
}

func (v *Values) Values() []Value {
	out := make([]Value, len(v.values))
	copy(out, v.values)
	return out
}

func (v *Values) AddValue(search, replace interface{}) {
	v.values = append(v.values, Value{Search: search, Replace: replace})
}

// AddKey replaces object Key with Key.
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

// AddId replaces id with id.
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

// AddContentField sets nested value in config/row.Content ordered map.
func (v *Values) AddContentField(objectKey model.Key, fieldPath orderedmap.Path, replace interface{}) {
	v.AddValue(ContentField{objectKey: objectKey, fieldPath: fieldPath}, replace)
}

func (v *Values) Replace(input interface{}) (interface{}, error) {
	if err := v.validate(); err != nil {
		return nil, err
	}

	return deepcopy.CopyTranslate(input, func(original, clone reflect.Value, steps deepcopy.Path) {
		for _, item := range v.values {
			switch v := item.Search.(type) {
			case ContentField:
				// Set nested value in config/row.Content ordered map
				if !original.IsValid() || original.IsZero() || !clone.IsValid() || clone.IsZero() {
					continue
				}
				originalObj, ok1 := original.Interface().(model.ObjectWithContent)
				cloneObj, ok2 := clone.Interface().(model.ObjectWithContent)
				if ok1 && ok2 && originalObj.Key() == v.objectKey {
					if err := cloneObj.GetContent().SetNestedPath(v.fieldPath, item.Replace); err != nil {
						panic(err)
					}
				}
			case SubString:
				// Search and replace sub-string
				if original.IsValid() && original.Type().String() == "string" {
					if modified, found := v.replace(original.String(), item.Replace.(string)); found {
						clone.Set(reflect.ValueOf(modified))
					}
				}
			default:
				// Replace other types
				if original.IsValid() && original.Interface() == item.Search {
					clone.Set(reflect.ValueOf(item.Replace))
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
	valuesMap := make(map[string]int)
	for _, item := range v.values {
		value := item.Search
		_, ok1 := value.(model.ConfigId)
		_, ok2 := value.(model.RowId)
		if ok1 || ok2 {
			valuesMap[cast.ToString(value)] += 1
		}
	}
	for k, count := range valuesMap {
		if count > 1 {
			return fmt.Errorf(`the old ID "%s" is defined %dx`, k, count)
		}
	}

	// New IDs must be unique
	valuesMap = make(map[string]int)
	for _, item := range v.values {
		value := item.Replace
		_, ok1 := value.(model.ConfigId)
		_, ok2 := value.(model.RowId)
		if ok1 || ok2 {
			valuesMap[cast.ToString(value)] += 1
		}
	}

	for k, count := range valuesMap {
		if count > 1 {
			return fmt.Errorf(`the new ID "%s" is defined %dx`, k, count)
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
