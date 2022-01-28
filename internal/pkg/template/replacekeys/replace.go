package replacekeys

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/deepcopy"
)

// Keys to be replaced.
type Keys []Key

// Key to be replaced.
type Key struct {
	Old model.Key
	New model.Key
}

// values to be replaced- processed Keys.
type values []value

// value to be replaced.
type value struct {
	Old interface{}
	New interface{}
}

type subString string

func (keys Keys) Values() (values, error) {
	var out values
	for _, item := range keys {
		switch v := item.Old.(type) {
		case model.BranchKey:
			// BranchKey
			out = append(out, value{
				Old: v,
				New: item.New.(model.BranchKey),
			})
			// BranchId
			out = append(out, value{
				Old: v.Id,
				New: item.New.(model.BranchKey).Id,
			})
		case model.ConfigKey:
			// ConfigKey
			out = append(out, value{
				Old: v,
				New: item.New.(model.ConfigKey),
			})
			// ConfigId
			out = append(out, value{
				Old: v.Id,
				New: item.New.(model.ConfigKey).Id,
			})
			// ConfigId in strings
			out = append(out, value{
				Old: subString(v.Id),
				New: string(item.New.(model.ConfigKey).Id),
			})
		case model.ConfigRowKey:
			// ConfigRowKey
			out = append(out, value{
				Old: v,
				New: item.New.(model.ConfigRowKey),
			})
			// ConfigRowId
			out = append(out, value{
				Old: v.Id,
				New: item.New.(model.ConfigRowKey).Id,
			})
			// ConfigRowId in strings
			out = append(out, value{
				Old: subString(v.Id),
				New: string(item.New.(model.ConfigRowKey).Id),
			})
		default:
			panic(fmt.Errorf(`unexpected key type "%T"`, item.Old))
		}
	}

	// Old IDs must be unique
	valueByString := make(map[interface{}][]interface{})
	for _, item := range out {
		valueByString[item.Old] = append(valueByString[item.Old], item.Old)
	}
	for k, v := range valueByString {
		if len(v) > 1 {
			return nil, fmt.Errorf(`the old ID "%s" is defined %dx`, k, len(v))
		}
	}

	// New IDs must be unique
	valueByString = make(map[interface{}][]interface{})
	for _, item := range out {
		valueByString[item.New] = append(valueByString[item.New], item.New)
	}
	for k, v := range valueByString {
		if len(v) > 1 {
			return nil, fmt.Errorf(`the new ID "%s" is defined %dx`, k, len(v))
		}
	}

	return out, nil
}

func ReplaceValues(replacement values, input interface{}) interface{} {
	return deepcopy.CopyTranslate(input, func(original, clone reflect.Value, steps deepcopy.Steps) {
		for _, item := range replacement {
			switch v := item.Old.(type) {
			case subString:
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
	})
}

func (s subString) replace(full, replacement string) (string, bool) {
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
