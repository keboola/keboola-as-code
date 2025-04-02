package primarykey

import (
	"reflect"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

const ParentEdgeName = "parent"

type Mixin struct {
	ent.Schema
	pkConfig
}

// Option used with the Definition function.
type Option func(*pkConfig)

type pkConfig struct {
	parent      ent.Interface
	parentMixin *Mixin
	pkFields    []pkFieldConfig
}

type pkFieldConfig struct {
	Name string
	Type any
}

// WithChildOf can be used at most once in a call of the Mixin Definition function.
// All primary keys of all parents will be included in the target primary key.
func WithChildOf(parent ent.Interface) Option {
	return func(config *pkConfig) {
		if config.parent != nil {
			panic(errors.Errorf(`primarykey.ChildOf option can be used at most once`))
		}
		config.parent = parent
	}
}

// WithField must be used at least once in a call of the Mixin Definition function.
// This option adds the field to the target primary key.
// Types can be any type that is based on string or int types.
func WithField(name string, typ any) Option {
	return func(config *pkConfig) {
		name = strhelper.FirstLower(name)
		config.pkFields = append(config.pkFields, pkFieldConfig{Name: name, Type: typ})
	}
}

// Definition creates primary key definition as an ent mixin.
// Read more: https://entgo.io/docs/schema-mixin
func Definition(ops ...Option) ent.Mixin {
	// Apply options
	c := pkConfig{}
	for _, o := range ops {
		o(&c)
	}

	// At lest one field is required
	if len(c.pkFields) == 0 {
		panic(errors.Errorf(`at least one field must be specified, please use primarykey.Field(name, type) option`))
	}

	// Merge PKs from the parent, if any
	if c.parent != nil {
		for _, mixin := range c.parent.Mixin() {
			if m, ok := mixin.(*Mixin); ok {
				c.parentMixin = m
				c.pkFields = append(m.pkFields, c.pkFields...)
			}
		}
	}

	return &Mixin{pkConfig: c}
}

// Fields method generates primary key partial fields.
func (v *Mixin) Fields() []ent.Field {
	var fields []ent.Field

	// Mark the ID field with the annotation and define GoType.
	// At the time of loading of this definition, there is no generated structure for the primary key yet.
	// So the KeyPlaceholder is used (it has same methods), and it is replaced by the generated struct in the Extension.Hooks.
	fields = append(fields, field.String("id").NotEmpty().Immutable().GoType(KeyPlaceholder{}).Annotations(v.annotation()))

	// Generate field for each primary key part.
	for _, f := range v.pkFields {
		a := f.annotation()
		switch a.GoType.Kind {
		case reflect.Int:
			fields = append(fields, field.Int(f.Name).GoType(f.Type).Min(1).Immutable().Annotations(a))
		case reflect.String:
			fields = append(fields, field.String(f.Name).GoType(f.Type).NotEmpty().Immutable().Annotations(a))
		default:
			panic(errors.Errorf(`unexpected field "%s" type "%s", expected a int or string based type`, f.Name, a.GoType.Name))
		}
	}

	return fields
}

// Edges method generates an edge to parent, if any.
func (v *Mixin) Edges() []ent.Edge {
	// Generate edge to parent, if any
	if v.parent != nil {
		typeMethod, ok := reflect.TypeOf(v.parent).MethodByName("Type")
		if !ok {
			panic(errors.Errorf(`method "Type" not found in "%T"`, v.parent))
		}

		// Enable cascade delete
		var annotations []schema.Annotation
		annotations = append(annotations, entsql.Annotation{OnDelete: entsql.Cascade})

		// Add annotation from the parent mixin, to set the parent fields together with the edge.
		if v.parentMixin != nil {
			annotations = append(annotations, v.parentMixin.annotation())
		}

		return []ent.Edge{
			edge.
				To(ParentEdgeName, typeMethod.Func.Interface()).
				Unique().
				Required().
				Immutable().
				Annotations(annotations...),
		}
	}
	return nil
}

// Indexes method generates index for composed primary key.
func (v *Mixin) Indexes() (indexes []ent.Index) {
	fields := make([]string, 0, len(v.pkFields))
	for _, f := range v.pkFields {
		fields = append(fields, f.Name)
		indexes = append(indexes, index.Fields(f.Name).Annotations(PKFieldAnnotation{}))
	}

	indexes = append(indexes, index.Fields(fields...).Unique().Annotations(PKComposedIndexAnnotation{}))
	return indexes
}
