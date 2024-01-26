package configpatch

import (
	"reflect"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
)

func matchTaggedFields(nameTags []string) configmap.OnField {
	return func(field reflect.StructField, path orderedmap.Path) (fieldName string, ok bool) {
		for _, nameTag := range nameTags {
			tagValue := field.Tag.Get(nameTag)
			fieldName, _, _ = strings.Cut(tagValue, ",")
			if fieldName != "" {
				break
			}
		}
		return fieldName, fieldName != ""
	}
}
