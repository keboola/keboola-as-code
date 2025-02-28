package replacevalues

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/deepcopy"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/cast"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Values struct {
	values    []Value
	validated bool
}

// Value to be replaced.
type Value struct {
	Search  any
	Replace any
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

func (v *Values) AddValue(search, replace any) {
	v.values = append(v.values, Value{Search: search, Replace: replace})
}

// AddKey replaces object Key with Key.
func (v *Values) AddKey(oldKey, newKey model.Key) {
	switch oldKey := oldKey.(type) {
	case model.BranchKey:
		v.AddValue(oldKey, newKey.(model.BranchKey))
		v.AddID(oldKey.ID, newKey.(model.BranchKey).ID)
	case model.ConfigKey:
		v.AddValue(oldKey, newKey.(model.ConfigKey))
		v.AddID(oldKey.ID, newKey.(model.ConfigKey).ID)
	case model.ConfigRowKey:
		v.AddValue(oldKey, newKey.(model.ConfigRowKey))
		v.AddID(oldKey.ID, newKey.(model.ConfigRowKey).ID)
	default:
		panic(errors.Errorf(`unexpected key type "%T"`, oldKey))
	}
}

// AddID replaces id with id.
func (v *Values) AddID(oldID, newID any) {
	switch old := oldID.(type) {
	case keboola.BranchID:
		v.AddValue(old, newID.(keboola.BranchID))
	case keboola.ConfigID:
		v.AddValue(old, newID.(keboola.ConfigID))
		// ConfigID in strings
		v.AddValue(SubString(old), string(newID.(keboola.ConfigID)))
	case keboola.RowID:
		v.AddValue(old, newID.(keboola.RowID))
		// ConfigRowId in strings
		v.AddValue(SubString(old), string(newID.(keboola.RowID)))
	default:
		panic(errors.Errorf(`unexpected ID type "%T"`, old))
	}
}

// AddContentField sets nested value in config/row.Content ordered map.
func (v *Values) AddContentField(objectKey model.Key, fieldPath orderedmap.Path, replace any) {
	v.AddValue(ContentField{objectKey: objectKey, fieldPath: fieldPath}, replace)
}

func (v *Values) Replace(input any) (any, error) {
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
		_, ok1 := value.(keboola.ConfigID)
		_, ok2 := value.(keboola.RowID)
		if ok1 || ok2 {
			valuesMap[cast.ToString(value)] += 1
		}
	}
	for k, count := range valuesMap {
		if count > 1 {
			return errors.Errorf(`the old ID "%s" is defined %dx`, k, count)
		}
	}

	// New IDs must be unique
	valuesMap = make(map[string]int)
	for _, item := range v.values {
		value := item.Replace
		_, ok1 := value.(keboola.ConfigID)
		_, ok2 := value.(keboola.RowID)
		if ok1 || ok2 {
			valuesMap[cast.ToString(value)] += 1
		}
	}

	for k, count := range valuesMap {
		if count > 1 {
			return errors.Errorf(`the new ID "%s" is defined %dx`, k, count)
		}
	}

	v.validated = true
	return nil
}

func (s SubString) replace(full, replacement string) (string, bool) {
	// nolint: govet
	re := regexpcache.MustCompile(fmt.Sprintf(
		`(^|[^a-zA-Z0-9])(` + // $1: start OR not alphanum
			`%s` + // $2: searched sub-string
			`)($|[^a-zA-Z0-9])`, // $3: end OR not alphanum
		regexp.QuoteMeta(string(s)),
	))
	if re.MatchString(full) {
		replacement = strings.ReplaceAll(replacement, `$`, `$$`)
		return re.ReplaceAllString(full, `${1}`+replacement+`${3}`), true
	}
	return "", false
}
