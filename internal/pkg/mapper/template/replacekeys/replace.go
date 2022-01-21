package replacekeys

import (
	"fmt"
	"reflect"

	"github.com/spf13/cast"

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

func (keys Keys) values() (values, error) {
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
		default:
			panic(fmt.Errorf(`unexpected key type "%T"`, item.Old))
		}
	}

	// Old IDs must be unique
	valueByString := make(map[string][]interface{})
	for _, item := range out {
		valueByString[cast.ToString(item.Old)] = append(valueByString[cast.ToString(item.Old)], item.Old)
	}
	for k, v := range valueByString {
		if len(v) > 1 {
			return nil, fmt.Errorf(`the old ID "%s" is defined %dx`, k, len(v))
		}
	}

	// New IDs must be unique
	valueByString = make(map[string][]interface{})
	for _, item := range out {
		valueByString[cast.ToString(item.New)] = append(valueByString[cast.ToString(item.New)], item.New)
	}
	for k, v := range valueByString {
		if len(v) > 1 {
			return nil, fmt.Errorf(`the new ID "%s" is defined %dx`, k, len(v))
		}
	}

	return out, nil
}

func replaceValues(replacement values, input interface{}) interface{} {
	return deepcopy.CopyTranslate(input, func(original, clone reflect.Value, steps deepcopy.Steps) {
		for _, item := range replacement {
			if original.IsValid() && original.Interface() == item.Old {
				clone.Set(reflect.ValueOf(item.New))
			}
		}
	})
}
