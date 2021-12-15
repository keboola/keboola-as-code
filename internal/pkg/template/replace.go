package template

import (
	"fmt"
	"reflect"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/deepcopy"
)

const IdRegexp = `^[a-zA-Z0-9\-]+$`

type ValuesReplacement []ValueReplacement

type ValueReplacement struct {
	Old interface{}
	New interface{}
}

type KeysReplacement []KeyReplacement

type KeyReplacement struct {
	Old model.Key
	New model.Key
}

func (keys KeysReplacement) Values() (ValuesReplacement, error) {
	var out ValuesReplacement
	for _, item := range keys {
		switch v := item.Old.(type) {
		case model.ConfigKey:
			// ConfigKey
			out = append(out, ValueReplacement{
				Old: v,
				New: item.New.(model.ConfigKey),
			})
			// ConfigId
			out = append(out, ValueReplacement{
				Old: v.Id,
				New: item.New.(model.ConfigKey).Id,
			})
		case model.ConfigRowKey:
			// ConfigRowKey
			out = append(out, ValueReplacement{
				Old: v,
				New: item.New.(model.ConfigRowKey),
			})
			// ConfigRowId
			out = append(out, ValueReplacement{
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

func replaceValues(replacement ValuesReplacement, value interface{}) interface{} {
	return deepcopy.CopyTranslate(value, func(clone reflect.Value, steps deepcopy.Steps) {
		for _, item := range replacement {
			if clone.IsValid() && clone.Interface() == item.Old {
				clone.Set(reflect.ValueOf(item.New))
			}
		}
	})
}
