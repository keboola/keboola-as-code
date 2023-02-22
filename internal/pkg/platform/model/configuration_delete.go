// Code generated by ent, DO NOT EDIT.

package model

import (
	"context"

	"entgo.io/ent/dialect/sql"
	"entgo.io/ent/dialect/sql/sqlgraph"
	"entgo.io/ent/schema/field"
	"github.com/keboola/keboola-as-code/internal/pkg/platform/model/configuration"
	"github.com/keboola/keboola-as-code/internal/pkg/platform/model/predicate"
)

// ConfigurationDelete is the builder for deleting a Configuration entity.
type ConfigurationDelete struct {
	config
	hooks    []Hook
	mutation *ConfigurationMutation
}

// Where appends a list predicates to the ConfigurationDelete builder.
func (cd *ConfigurationDelete) Where(ps ...predicate.Configuration) *ConfigurationDelete {
	cd.mutation.Where(ps...)
	return cd
}

// Exec executes the deletion query and returns how many vertices were deleted.
func (cd *ConfigurationDelete) Exec(ctx context.Context) (int, error) {
	return withHooks[int, ConfigurationMutation](ctx, cd.sqlExec, cd.mutation, cd.hooks)
}

// ExecX is like Exec, but panics if an error occurs.
func (cd *ConfigurationDelete) ExecX(ctx context.Context) int {
	n, err := cd.Exec(ctx)
	if err != nil {
		panic(err)
	}
	return n
}

func (cd *ConfigurationDelete) sqlExec(ctx context.Context) (int, error) {
	_spec := &sqlgraph.DeleteSpec{
		Node: &sqlgraph.NodeSpec{
			Table: configuration.Table,
			ID: &sqlgraph.FieldSpec{
				Type:   field.TypeString,
				Column: configuration.FieldID,
			},
		},
	}
	if ps := cd.mutation.predicates; len(ps) > 0 {
		_spec.Predicate = func(selector *sql.Selector) {
			for i := range ps {
				ps[i](selector)
			}
		}
	}
	affected, err := sqlgraph.DeleteNodes(ctx, cd.driver, _spec)
	if err != nil && sqlgraph.IsConstraintError(err) {
		err = &ConstraintError{msg: err.Error(), wrap: err}
	}
	cd.mutation.done = true
	return affected, err
}

// ConfigurationDeleteOne is the builder for deleting a single Configuration entity.
type ConfigurationDeleteOne struct {
	cd *ConfigurationDelete
}

// Where appends a list predicates to the ConfigurationDelete builder.
func (cdo *ConfigurationDeleteOne) Where(ps ...predicate.Configuration) *ConfigurationDeleteOne {
	cdo.cd.mutation.Where(ps...)
	return cdo
}

// Exec executes the deletion query.
func (cdo *ConfigurationDeleteOne) Exec(ctx context.Context) error {
	n, err := cdo.cd.Exec(ctx)
	switch {
	case err != nil:
		return err
	case n == 0:
		return &NotFoundError{configuration.Label}
	default:
		return nil
	}
}

// ExecX is like Exec, but panics if an error occurs.
func (cdo *ConfigurationDeleteOne) ExecX(ctx context.Context) {
	if err := cdo.Exec(ctx); err != nil {
		panic(err)
	}
}
