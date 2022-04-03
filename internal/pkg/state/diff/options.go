package diff

import (
	"github.com/google/go-cmp/cmp"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

// options to modify diff process.
func options(r *Reporter) cmp.Options {
	return cmp.Options{
		cmp.Reporter(r),
		// Diff only struct fields with diff:"true" tag
		onlyFiledWithDiffTag(),
		// Compare ordered map as native map (keys order doesn't matter)
		orderedMapToMapTransformer(),
		// Convert []Object -> Object, if parent Object can have only one child Object of a Kind.
		// Convert []Object -> map[Key]Object, so objects with the same key are compared with each other.
		objectsSliceTransformer(),
	}
}

// onlyFiledWithDiffTag ignores struct field without diff:"true" tag
func onlyFiledWithDiffTag() cmp.Option {
	return cmp.FilterPath(
		func(path cmp.Path) bool {
			previousIndex := len(path) - 2
			if previousIndex > 0 {
				if v, ok := path.Last().(cmp.StructField); ok {
					parentType := path.Index(len(path) - 2).Type()
					currentField, _ := parentType.FieldByName(v.Name())
					return currentField.Tag.Get("diff") != "true"
				}
			}
			// Allow
			return false
		},
		cmp.Ignore(),
	)
}

// orderedMapToMapTransformer converts "orderedmap" type to native map, so keys order doesn't matter.
func orderedMapToMapTransformer() cmp.Option {
	return cmp.Transformer("orderedMap", func(m *orderedmap.OrderedMap) map[string]interface{} {
		return m.ToMap()
	})
}

// objectsSliceTransformer has 2 functions
// 1: Converts []Object -> Object, if parent Object can have only one child Object of a Kind.
// 2: Converts []Object -> map[Key]Object, so objects with the same key are compared.
func objectsSliceTransformer() cmp.Option {
	return cmp.Transformer("objectsSliceToMap", func(children []*model.ObjectNode) interface{} {
		// Convert []Object -> Object, if parent Object can have only one child Object of a Kind.
		if len(children) == 1 && !children[0].Kind().ToMany {
			return children[0]
		}

		// Convert []Object -> Object, if parent Object can have only one child Object of a Kind.
		out := make(map[model.Key]*model.ObjectNode)
		for _, o := range children {
			out[o.Key()] = o
		}
		return out
	})
}
