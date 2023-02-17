// Package fixture contains simple schema for tests.
package fixture

import (
	"entgo.io/ent"

	"github.com/keboola/keboola-as-code/internal/pkg/platform/schema/compiler/extension/primarykey"
)

type SomeString string

type SomeInt int

type Parent struct {
	ent.Schema
}

type Child struct {
	ent.Schema
}

type SubChild struct {
	ent.Schema
}

func (Child) Mixin() []ent.Mixin {
	return []ent.Mixin{
		primarykey.Definition(
			primarykey.WithChildOf(Parent{}),
			primarykey.WithField("myID", SomeString("")),
		),
	}
}

func (SubChild) Mixin() []ent.Mixin {
	return []ent.Mixin{
		primarykey.Definition(
			primarykey.WithChildOf(Child{}),
			primarykey.WithField("groupID", SomeString("")),
			primarykey.WithField("categoryID", SomeInt(0)),
		),
	}
}
