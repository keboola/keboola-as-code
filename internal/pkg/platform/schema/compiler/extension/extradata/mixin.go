// Package extradata provide the Mixin for various additional entity fields.
package extradata

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"github.com/keboola/go-utils/pkg/orderedmap"
)

type Mixin struct {
	ent.Schema
}

func (v *Mixin) Fields() []ent.Field {
	return []ent.Field{
		field.
			JSON("extra", &orderedmap.OrderedMap{}).
			Comment("Extra data contains all entity fields that do not match the entity schema on parsing, used for forward compatibility").
			Optional(),
	}
}
