package primarykey_test

import (
	"reflect"
	"testing"

	"entgo.io/ent/schema"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/platform/schema/compiler/extension/primarykey"
	"github.com/keboola/keboola-as-code/internal/pkg/platform/schema/compiler/extension/primarykey/fixture"
)

func TestDefinition_Empty(t *testing.T) {
	t.Parallel()
	assert.PanicsWithError(t, "at least one field must be specified, please use primarykey.Field(name, type) option", func() {
		primarykey.Definition()
	})
}

func TestDefinition_MultipleChildOf(t *testing.T) {
	t.Parallel()
	assert.PanicsWithError(t, "primarykey.ChildOf option can be used at most once", func() {
		primarykey.Definition(
			primarykey.WithField("myID", fixture.SomeString("")),
			primarykey.WithChildOf(fixture.Parent{}),
			primarykey.WithChildOf(fixture.Parent{}),
		)
	})
}

func TestDefinition_Valid_OneField(t *testing.T) {
	t.Parallel()
	mixin := primarykey.Definition(
		primarykey.WithField("myID", fixture.SomeString("")),
	)

	fields := mixin.Fields()
	assert.Len(t, fields, 2)
	edges := mixin.Edges()
	assert.Empty(t, edges)

	// ID field
	assert.Equal(t, "id", fields[0].Descriptor().Name)
	assert.True(t, fields[0].Descriptor().Immutable)
	assert.False(t, fields[0].Descriptor().Optional)
	assert.Equal(t, []schema.Annotation{
		primarykey.PKAnnotation{
			Fields: []primarykey.PKFieldAnnotation{
				{
					PublicName:  "MyID",
					PrivateName: "myID",
					GoType: primarykey.GoType{
						Name:    "fixture.SomeString",
						Kind:    reflect.String,
						KindStr: "string",
						PkgPath: "github.com/keboola/keboola-as-code/internal/pkg/platform/schema/compiler/extension/primarykey/fixture",
					},
				},
			},
		},
	}, fields[0].Descriptor().Annotations)

	// Generated myID field
	assert.Equal(t, "myID", fields[1].Descriptor().Name)
	assert.True(t, fields[1].Descriptor().Immutable)
	assert.False(t, fields[1].Descriptor().Optional)
	assert.Equal(t, []schema.Annotation{
		primarykey.PKFieldAnnotation{
			PublicName:  "MyID",
			PrivateName: "myID",
			GoType: primarykey.GoType{
				Name:    "fixture.SomeString",
				Kind:    reflect.String,
				KindStr: "string",
				PkgPath: "github.com/keboola/keboola-as-code/internal/pkg/platform/schema/compiler/extension/primarykey/fixture",
			},
		},
	}, fields[1].Descriptor().Annotations)
}

func TestDefinition_Valid_MoreFields(t *testing.T) {
	t.Parallel()
	mixin := primarykey.Definition(
		primarykey.WithField("groupID", fixture.SomeString("")),
		primarykey.WithField("categoryID", fixture.SomeInt(0)),
	)

	fields := mixin.Fields()
	assert.Len(t, fields, 3)
	edges := mixin.Edges()
	assert.Empty(t, edges)

	// ID field
	assert.Equal(t, "id", fields[0].Descriptor().Name)
	assert.True(t, fields[0].Descriptor().Immutable)
	assert.False(t, fields[0].Descriptor().Optional)
	assert.Equal(t, []schema.Annotation{
		primarykey.PKAnnotation{
			Fields: []primarykey.PKFieldAnnotation{
				{
					PublicName:  "GroupID",
					PrivateName: "groupID",
					GoType: primarykey.GoType{
						Name:    "fixture.SomeString",
						Kind:    reflect.String,
						KindStr: "string",
						PkgPath: "github.com/keboola/keboola-as-code/internal/pkg/platform/schema/compiler/extension/primarykey/fixture",
					},
				},
				{
					PublicName:  "CategoryID",
					PrivateName: "categoryID",
					GoType: primarykey.GoType{
						Name:    "fixture.SomeInt",
						Kind:    reflect.Int,
						KindStr: "int",
						PkgPath: "github.com/keboola/keboola-as-code/internal/pkg/platform/schema/compiler/extension/primarykey/fixture",
					},
				},
			},
		},
	}, fields[0].Descriptor().Annotations)

	// Generated groupID field
	assert.Equal(t, "groupID", fields[1].Descriptor().Name)
	assert.True(t, fields[1].Descriptor().Immutable)
	assert.False(t, fields[1].Descriptor().Optional)
	assert.Equal(t, []schema.Annotation{
		primarykey.PKFieldAnnotation{
			PublicName:  "GroupID",
			PrivateName: "groupID",
			GoType: primarykey.GoType{
				Name:    "fixture.SomeString",
				Kind:    reflect.String,
				KindStr: "string",
				PkgPath: "github.com/keboola/keboola-as-code/internal/pkg/platform/schema/compiler/extension/primarykey/fixture",
			},
		},
	}, fields[1].Descriptor().Annotations)

	// Generated categoryID field
	assert.Equal(t, "categoryID", fields[2].Descriptor().Name)
	assert.True(t, fields[2].Descriptor().Immutable)
	assert.False(t, fields[2].Descriptor().Optional)
	assert.Equal(t, []schema.Annotation{
		primarykey.PKFieldAnnotation{
			PublicName:  "CategoryID",
			PrivateName: "categoryID",
			GoType: primarykey.GoType{
				Name:    "fixture.SomeInt",
				Kind:    reflect.Int,
				KindStr: "int",
				PkgPath: "github.com/keboola/keboola-as-code/internal/pkg/platform/schema/compiler/extension/primarykey/fixture",
			},
		},
	}, fields[2].Descriptor().Annotations)
}

func TestDefinition_Valid_Complex(t *testing.T) {
	t.Parallel()
	mixin := fixture.SubChild{}.Mixin()[0]

	fields := mixin.Fields()
	assert.Len(t, fields, 4) // 1 ID, 0 from Parent, 1 from Child, 2 from SubChild primary keys
	edges := mixin.Edges()
	assert.Len(t, edges, 1)

	// ID field
	assert.Equal(t, "id", fields[0].Descriptor().Name)
	assert.True(t, fields[0].Descriptor().Immutable)
	assert.False(t, fields[0].Descriptor().Optional)
	assert.Equal(t, []schema.Annotation{
		primarykey.PKAnnotation{
			Fields: []primarykey.PKFieldAnnotation{
				{
					PublicName:  "MyID",
					PrivateName: "myID",
					GoType: primarykey.GoType{
						Name:    "fixture.SomeString",
						Kind:    reflect.String,
						KindStr: "string",
						PkgPath: "github.com/keboola/keboola-as-code/internal/pkg/platform/schema/compiler/extension/primarykey/fixture",
					},
				},
				{
					PublicName:  "GroupID",
					PrivateName: "groupID",
					GoType: primarykey.GoType{
						Name:    "fixture.SomeString",
						Kind:    reflect.String,
						KindStr: "string",
						PkgPath: "github.com/keboola/keboola-as-code/internal/pkg/platform/schema/compiler/extension/primarykey/fixture",
					},
				},
				{
					PublicName:  "CategoryID",
					PrivateName: "categoryID",
					GoType: primarykey.GoType{
						Name:    "fixture.SomeInt",
						Kind:    reflect.Int,
						KindStr: "int",
						PkgPath: "github.com/keboola/keboola-as-code/internal/pkg/platform/schema/compiler/extension/primarykey/fixture",
					},
				},
			},
		},
	}, fields[0].Descriptor().Annotations)

	// Generated myID field
	assert.Equal(t, "myID", fields[1].Descriptor().Name)
	assert.True(t, fields[1].Descriptor().Immutable)
	assert.False(t, fields[1].Descriptor().Optional)
	assert.Equal(t, []schema.Annotation{
		primarykey.PKFieldAnnotation{
			PublicName:  "MyID",
			PrivateName: "myID",
			GoType: primarykey.GoType{
				Name:    "fixture.SomeString",
				Kind:    reflect.String,
				KindStr: "string",
				PkgPath: "github.com/keboola/keboola-as-code/internal/pkg/platform/schema/compiler/extension/primarykey/fixture",
			},
		},
	}, fields[1].Descriptor().Annotations)

	// Generated groupID field
	assert.Equal(t, "groupID", fields[2].Descriptor().Name)
	assert.True(t, fields[2].Descriptor().Immutable)
	assert.False(t, fields[2].Descriptor().Optional)
	assert.Equal(t, []schema.Annotation{
		primarykey.PKFieldAnnotation{
			PublicName:  "GroupID",
			PrivateName: "groupID",
			GoType: primarykey.GoType{
				Name:    "fixture.SomeString",
				Kind:    reflect.String,
				KindStr: "string",
				PkgPath: "github.com/keboola/keboola-as-code/internal/pkg/platform/schema/compiler/extension/primarykey/fixture",
			},
		},
	}, fields[2].Descriptor().Annotations)

	// Generated categoryID field
	assert.Equal(t, "categoryID", fields[3].Descriptor().Name)
	assert.True(t, fields[3].Descriptor().Immutable)
	assert.False(t, fields[3].Descriptor().Optional)
	assert.Equal(t, []schema.Annotation{
		primarykey.PKFieldAnnotation{
			PublicName:  "CategoryID",
			PrivateName: "categoryID",
			GoType: primarykey.GoType{
				Name:    "fixture.SomeInt",
				Kind:    reflect.Int,
				KindStr: "int",
				PkgPath: "github.com/keboola/keboola-as-code/internal/pkg/platform/schema/compiler/extension/primarykey/fixture",
			},
		},
	}, fields[3].Descriptor().Annotations)

	// Generated edge to parent
	assert.Equal(t, "parent", edges[0].Descriptor().Name)
	assert.True(t, edges[0].Descriptor().Immutable)
	assert.True(t, edges[0].Descriptor().Required)
}
