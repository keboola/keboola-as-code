package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/platform/schema/compiler/extension/primarykey"
)

type Configuration struct {
	ent.Schema
}

func (Configuration) Mixin() []ent.Mixin {
	return []ent.Mixin{
		primarykey.Definition(
			primarykey.WithChildOf(Branch{}),
			primarykey.WithField("componentID", keboola.ComponentID("")),
			primarykey.WithField("configID", keboola.ConfigID("")),
		),
	}
}

func (Configuration) Fields() []ent.Field {
	return []ent.Field{
		field.String("name").NotEmpty(),
		field.String("description"),
		field.Bool("isDisabled").Default(false),
		field.JSON("content", &orderedmap.OrderedMap{}),
	}
}
