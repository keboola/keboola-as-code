package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/platform/schema/compiler/extension/primarykey"
)

type ConfigurationRow struct {
	ent.Schema
}

func (ConfigurationRow) Mixin() []ent.Mixin {
	return []ent.Mixin{
		primarykey.Definition(
			primarykey.WithChildOf(Configuration{}),
			primarykey.WithField("rowID", keboola.RowID("")),
		),
	}
}

func (ConfigurationRow) Fields() []ent.Field {
	return []ent.Field{
		field.String("name").NotEmpty(),
		field.String("description"),
		field.Bool("isDisabled").Default(false).Comment("If IsDisabled=true, then when the entire configuration is run, the row will be skipped."),
		field.JSON("content", &orderedmap.OrderedMap{}),
	}
}
