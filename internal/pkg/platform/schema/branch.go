package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/platform/schema/compiler/extension/primarykey"
)

type Branch struct {
	ent.Schema
}

func (Branch) Mixin() []ent.Mixin {
	return []ent.Mixin{
		primarykey.Definition(
			primarykey.WithField("branchID", keboola.BranchID(0)),
		),
	}
}

func (Branch) Fields() []ent.Field {
	return []ent.Field{
		field.String("name").NotEmpty(),
		field.String("description"),
		field.Bool("isDefault").Immutable(),
	}
}

func (Branch) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("configurations", Configuration.Type).Ref("parent"),
	}
}
