// Package transformer modifies how the diff will be displayed. It converts objects into a more readable form.
package transformer

import (
	"github.com/google/go-cmp/cmp"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type Transformer struct {
	naming *naming.Registry
}

func NewTransformer(naming *naming.Registry) *Transformer {
	return &Transformer{naming: naming}
}

func (t *Transformer) Options() cmp.Options {
	return cmp.Options{
		// Diff only struct fields with diff:"true" tag
		onlyFiledWithDiffTag(),
		// Compare ordered map as native map (keys order doesn't matter)
		orderedMapToMapTransformer(),
		// Convert []Object -> Object, if parent Object can have only one child Object of a Kind.
		oneChildTransformer(),
		// Convert []Object -> map[Key]Object, so objects with the same key are compared.
		compareObjectsByKeysTransformer(),
		// Transform object before comparison
		t.objectsTransformer(),
	}
}

// transformObject before comparison if needed.
func (t *Transformer) transformObject(v *model.ObjectLeaf) interface{} {
	if _, ok := v.Object.(*model.Transformation); ok {
		return t.transformationToString(v)
	}

	if _, ok := v.Object.(*model.Orchestration); ok {
		return t.orchestrationToString(v)
	}

	return v
}

func (t *Transformer) objectsTransformer() cmp.Option {
	return onlyOnceTransformer("transformObject", t.transformObject)
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

// oneChildTransformer converts []Object -> Object, if parent Object can have only one child Object of a Kind.
func oneChildTransformer() cmp.Option {
	return onlyOnceTransformer("oneChild", func(children []*model.ObjectLeaf) interface{} {
		if len(children) == 1 && !children[0].Kind().ToMany {
			return children[0]
		}
		return children
	})
}

// compareObjectsByKeysTransformer converts []Object -> map[Key]Object, so objects with the same key are compared.
func compareObjectsByKeysTransformer() cmp.Option {
	return cmp.Transformer("objectsSliceToMap", func(children []*model.ObjectLeaf) interface{} {
		out := make(map[model.Key]*model.ObjectLeaf)
		for _, o := range children {
			out[o.Key()] = o
		}
		return out
	})
}

// onlyOnceTransformer option prevents run of the transformer twice in row.
// This could happen if the value type has not changed.
func onlyOnceTransformer(name string, f interface{}) cmp.Option {
	return cmp.FilterPath(
		func(path cmp.Path) bool {
			previousIndex := len(path) - 2
			if previousIndex > 0 {
				if step, ok := path[previousIndex].(cmp.Transform); ok {
					return step.Name() != name
				}
			}
			return true
		},
		cmp.Transformer(name, f),
	)
}
