// Package transformer modifies how the diff will be displayed. It converts objects into a more readable form.
package transformer

import (
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

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
		// Compare Config/ConfigRow configuration content ("orderedmap" type) as map (keys order doesn't matter)
		cmp.Transformer("orderedmap", func(m *orderedmap.OrderedMap) map[string]interface{} {
			return m.ToMap()
		}),
		// Separately compares the relations for the manifest and API side
		cmpopts.AcyclicTransformer("relations", func(relations model.Relations) model.RelationsBySide {
			return relations.RelationsBySide()
		}),
		// Separately compares the relations for the manifest and API side
		cmpopts.AcyclicTransformer("object", t.transformObject),
		cmpopts.AcyclicTransformer("aaa", func(objects []*model.ObjectLeaf) interface{} {
			if len(objects) == 1 && !objects[0].Kind().ToMany {
				return objects[0]
			}
			return objects
		}),
	}
}

func (t *Transformer) transformObject(v *model.ObjectLeaf) string {
	if _, ok := v.Object.(*model.Transformation); ok {
		return t.transformationToString(v)
	}

	if _, ok := v.Object.(*model.Orchestration); ok {
		return t.orchestrationToString(v)
	}

	return v.Object.String()
}
