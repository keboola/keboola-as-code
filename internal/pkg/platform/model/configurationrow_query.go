// Code generated by ent, DO NOT EDIT.

package model

import (
	"context"
	"fmt"
	"math"

	"entgo.io/ent"
	"entgo.io/ent/dialect/sql"
	"entgo.io/ent/dialect/sql/sqlgraph"
	"entgo.io/ent/schema/field"
	"github.com/keboola/keboola-as-code/internal/pkg/platform/model/configuration"
	"github.com/keboola/keboola-as-code/internal/pkg/platform/model/configurationrow"
	"github.com/keboola/keboola-as-code/internal/pkg/platform/model/key"
	"github.com/keboola/keboola-as-code/internal/pkg/platform/model/predicate"
)

// ConfigurationRowQuery is the builder for querying ConfigurationRow entities.
type ConfigurationRowQuery struct {
	config
	ctx        *QueryContext
	order      []configurationrow.OrderOption
	inters     []Interceptor
	predicates []predicate.ConfigurationRow
	withParent *ConfigurationQuery
	withFKs    bool
	// intermediate query (i.e. traversal path).
	sql  *sql.Selector
	path func(context.Context) (*sql.Selector, error)
}

// Where adds a new predicate for the ConfigurationRowQuery builder.
func (crq *ConfigurationRowQuery) Where(ps ...predicate.ConfigurationRow) *ConfigurationRowQuery {
	crq.predicates = append(crq.predicates, ps...)
	return crq
}

// Limit the number of records to be returned by this query.
func (crq *ConfigurationRowQuery) Limit(limit int) *ConfigurationRowQuery {
	crq.ctx.Limit = &limit
	return crq
}

// Offset to start from.
func (crq *ConfigurationRowQuery) Offset(offset int) *ConfigurationRowQuery {
	crq.ctx.Offset = &offset
	return crq
}

// Unique configures the query builder to filter duplicate records on query.
// By default, unique is set to true, and can be disabled using this method.
func (crq *ConfigurationRowQuery) Unique(unique bool) *ConfigurationRowQuery {
	crq.ctx.Unique = &unique
	return crq
}

// Order specifies how the records should be ordered.
func (crq *ConfigurationRowQuery) Order(o ...configurationrow.OrderOption) *ConfigurationRowQuery {
	crq.order = append(crq.order, o...)
	return crq
}

// QueryParent chains the current query on the "parent" edge.
func (crq *ConfigurationRowQuery) QueryParent() *ConfigurationQuery {
	query := (&ConfigurationClient{config: crq.config}).Query()
	query.path = func(ctx context.Context) (fromU *sql.Selector, err error) {
		if err := crq.prepareQuery(ctx); err != nil {
			return nil, err
		}
		selector := crq.sqlQuery(ctx)
		if err := selector.Err(); err != nil {
			return nil, err
		}
		step := sqlgraph.NewStep(
			sqlgraph.From(configurationrow.Table, configurationrow.FieldID, selector),
			sqlgraph.To(configuration.Table, configuration.FieldID),
			sqlgraph.Edge(sqlgraph.M2O, false, configurationrow.ParentTable, configurationrow.ParentColumn),
		)
		fromU = sqlgraph.SetNeighbors(crq.driver.Dialect(), step)
		return fromU, nil
	}
	return query
}

// First returns the first ConfigurationRow entity from the query.
// Returns a *NotFoundError when no ConfigurationRow was found.
func (crq *ConfigurationRowQuery) First(ctx context.Context) (*ConfigurationRow, error) {
	nodes, err := crq.Limit(1).All(setContextOp(ctx, crq.ctx, ent.OpQueryFirst))
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, &NotFoundError{configurationrow.Label}
	}
	return nodes[0], nil
}

// FirstX is like First, but panics if an error occurs.
func (crq *ConfigurationRowQuery) FirstX(ctx context.Context) *ConfigurationRow {
	node, err := crq.First(ctx)
	if err != nil && !IsNotFound(err) {
		panic(err)
	}
	return node
}

// FirstID returns the first ConfigurationRow ID from the query.
// Returns a *NotFoundError when no ConfigurationRow ID was found.
func (crq *ConfigurationRowQuery) FirstID(ctx context.Context) (id key.ConfigurationRowKey, err error) {
	var ids []key.ConfigurationRowKey
	if ids, err = crq.Limit(1).IDs(setContextOp(ctx, crq.ctx, ent.OpQueryFirstID)); err != nil {
		return
	}
	if len(ids) == 0 {
		err = &NotFoundError{configurationrow.Label}
		return
	}
	return ids[0], nil
}

// FirstIDX is like FirstID, but panics if an error occurs.
func (crq *ConfigurationRowQuery) FirstIDX(ctx context.Context) key.ConfigurationRowKey {
	id, err := crq.FirstID(ctx)
	if err != nil && !IsNotFound(err) {
		panic(err)
	}
	return id
}

// Only returns a single ConfigurationRow entity found by the query, ensuring it only returns one.
// Returns a *NotSingularError when more than one ConfigurationRow entity is found.
// Returns a *NotFoundError when no ConfigurationRow entities are found.
func (crq *ConfigurationRowQuery) Only(ctx context.Context) (*ConfigurationRow, error) {
	nodes, err := crq.Limit(2).All(setContextOp(ctx, crq.ctx, ent.OpQueryOnly))
	if err != nil {
		return nil, err
	}
	switch len(nodes) {
	case 1:
		return nodes[0], nil
	case 0:
		return nil, &NotFoundError{configurationrow.Label}
	default:
		return nil, &NotSingularError{configurationrow.Label}
	}
}

// OnlyX is like Only, but panics if an error occurs.
func (crq *ConfigurationRowQuery) OnlyX(ctx context.Context) *ConfigurationRow {
	node, err := crq.Only(ctx)
	if err != nil {
		panic(err)
	}
	return node
}

// OnlyID is like Only, but returns the only ConfigurationRow ID in the query.
// Returns a *NotSingularError when more than one ConfigurationRow ID is found.
// Returns a *NotFoundError when no entities are found.
func (crq *ConfigurationRowQuery) OnlyID(ctx context.Context) (id key.ConfigurationRowKey, err error) {
	var ids []key.ConfigurationRowKey
	if ids, err = crq.Limit(2).IDs(setContextOp(ctx, crq.ctx, ent.OpQueryOnlyID)); err != nil {
		return
	}
	switch len(ids) {
	case 1:
		id = ids[0]
	case 0:
		err = &NotFoundError{configurationrow.Label}
	default:
		err = &NotSingularError{configurationrow.Label}
	}
	return
}

// OnlyIDX is like OnlyID, but panics if an error occurs.
func (crq *ConfigurationRowQuery) OnlyIDX(ctx context.Context) key.ConfigurationRowKey {
	id, err := crq.OnlyID(ctx)
	if err != nil {
		panic(err)
	}
	return id
}

// All executes the query and returns a list of ConfigurationRows.
func (crq *ConfigurationRowQuery) All(ctx context.Context) ([]*ConfigurationRow, error) {
	ctx = setContextOp(ctx, crq.ctx, ent.OpQueryAll)
	if err := crq.prepareQuery(ctx); err != nil {
		return nil, err
	}
	qr := querierAll[[]*ConfigurationRow, *ConfigurationRowQuery]()
	return withInterceptors[[]*ConfigurationRow](ctx, crq, qr, crq.inters)
}

// AllX is like All, but panics if an error occurs.
func (crq *ConfigurationRowQuery) AllX(ctx context.Context) []*ConfigurationRow {
	nodes, err := crq.All(ctx)
	if err != nil {
		panic(err)
	}
	return nodes
}

// IDs executes the query and returns a list of ConfigurationRow IDs.
func (crq *ConfigurationRowQuery) IDs(ctx context.Context) (ids []key.ConfigurationRowKey, err error) {
	if crq.ctx.Unique == nil && crq.path != nil {
		crq.Unique(true)
	}
	ctx = setContextOp(ctx, crq.ctx, ent.OpQueryIDs)
	if err = crq.Select(configurationrow.FieldID).Scan(ctx, &ids); err != nil {
		return nil, err
	}
	return ids, nil
}

// IDsX is like IDs, but panics if an error occurs.
func (crq *ConfigurationRowQuery) IDsX(ctx context.Context) []key.ConfigurationRowKey {
	ids, err := crq.IDs(ctx)
	if err != nil {
		panic(err)
	}
	return ids
}

// Count returns the count of the given query.
func (crq *ConfigurationRowQuery) Count(ctx context.Context) (int, error) {
	ctx = setContextOp(ctx, crq.ctx, ent.OpQueryCount)
	if err := crq.prepareQuery(ctx); err != nil {
		return 0, err
	}
	return withInterceptors[int](ctx, crq, querierCount[*ConfigurationRowQuery](), crq.inters)
}

// CountX is like Count, but panics if an error occurs.
func (crq *ConfigurationRowQuery) CountX(ctx context.Context) int {
	count, err := crq.Count(ctx)
	if err != nil {
		panic(err)
	}
	return count
}

// Exist returns true if the query has elements in the graph.
func (crq *ConfigurationRowQuery) Exist(ctx context.Context) (bool, error) {
	ctx = setContextOp(ctx, crq.ctx, ent.OpQueryExist)
	switch _, err := crq.FirstID(ctx); {
	case IsNotFound(err):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("model: check existence: %w", err)
	default:
		return true, nil
	}
}

// ExistX is like Exist, but panics if an error occurs.
func (crq *ConfigurationRowQuery) ExistX(ctx context.Context) bool {
	exist, err := crq.Exist(ctx)
	if err != nil {
		panic(err)
	}
	return exist
}

// Clone returns a duplicate of the ConfigurationRowQuery builder, including all associated steps. It can be
// used to prepare common query builders and use them differently after the clone is made.
func (crq *ConfigurationRowQuery) Clone() *ConfigurationRowQuery {
	if crq == nil {
		return nil
	}
	return &ConfigurationRowQuery{
		config:     crq.config,
		ctx:        crq.ctx.Clone(),
		order:      append([]configurationrow.OrderOption{}, crq.order...),
		inters:     append([]Interceptor{}, crq.inters...),
		predicates: append([]predicate.ConfigurationRow{}, crq.predicates...),
		withParent: crq.withParent.Clone(),
		// clone intermediate query.
		sql:  crq.sql.Clone(),
		path: crq.path,
	}
}

// WithParent tells the query-builder to eager-load the nodes that are connected to
// the "parent" edge. The optional arguments are used to configure the query builder of the edge.
func (crq *ConfigurationRowQuery) WithParent(opts ...func(*ConfigurationQuery)) *ConfigurationRowQuery {
	query := (&ConfigurationClient{config: crq.config}).Query()
	for _, opt := range opts {
		opt(query)
	}
	crq.withParent = query
	return crq
}

// GroupBy is used to group vertices by one or more fields/columns.
// It is often used with aggregate functions, like: count, max, mean, min, sum.
//
// Example:
//
//	var v []struct {
//		BranchID keboola.BranchID `json:"branchID,omitempty"`
//		Count int `json:"count,omitempty"`
//	}
//
//	client.ConfigurationRow.Query().
//		GroupBy(configurationrow.FieldBranchID).
//		Aggregate(model.Count()).
//		Scan(ctx, &v)
func (crq *ConfigurationRowQuery) GroupBy(field string, fields ...string) *ConfigurationRowGroupBy {
	crq.ctx.Fields = append([]string{field}, fields...)
	grbuild := &ConfigurationRowGroupBy{build: crq}
	grbuild.flds = &crq.ctx.Fields
	grbuild.label = configurationrow.Label
	grbuild.scan = grbuild.Scan
	return grbuild
}

// Select allows the selection one or more fields/columns for the given query,
// instead of selecting all fields in the entity.
//
// Example:
//
//	var v []struct {
//		BranchID keboola.BranchID `json:"branchID,omitempty"`
//	}
//
//	client.ConfigurationRow.Query().
//		Select(configurationrow.FieldBranchID).
//		Scan(ctx, &v)
func (crq *ConfigurationRowQuery) Select(fields ...string) *ConfigurationRowSelect {
	crq.ctx.Fields = append(crq.ctx.Fields, fields...)
	sbuild := &ConfigurationRowSelect{ConfigurationRowQuery: crq}
	sbuild.label = configurationrow.Label
	sbuild.flds, sbuild.scan = &crq.ctx.Fields, sbuild.Scan
	return sbuild
}

// Aggregate returns a ConfigurationRowSelect configured with the given aggregations.
func (crq *ConfigurationRowQuery) Aggregate(fns ...AggregateFunc) *ConfigurationRowSelect {
	return crq.Select().Aggregate(fns...)
}

func (crq *ConfigurationRowQuery) prepareQuery(ctx context.Context) error {
	for _, inter := range crq.inters {
		if inter == nil {
			return fmt.Errorf("model: uninitialized interceptor (forgotten import model/runtime?)")
		}
		if trv, ok := inter.(Traverser); ok {
			if err := trv.Traverse(ctx, crq); err != nil {
				return err
			}
		}
	}
	for _, f := range crq.ctx.Fields {
		if !configurationrow.ValidColumn(f) {
			return &ValidationError{Name: f, err: fmt.Errorf("model: invalid field %q for query", f)}
		}
	}
	if crq.path != nil {
		prev, err := crq.path(ctx)
		if err != nil {
			return err
		}
		crq.sql = prev
	}
	return nil
}

func (crq *ConfigurationRowQuery) sqlAll(ctx context.Context, hooks ...queryHook) ([]*ConfigurationRow, error) {
	var (
		nodes       = []*ConfigurationRow{}
		withFKs     = crq.withFKs
		_spec       = crq.querySpec()
		loadedTypes = [1]bool{
			crq.withParent != nil,
		}
	)
	if crq.withParent != nil {
		withFKs = true
	}
	if withFKs {
		_spec.Node.Columns = append(_spec.Node.Columns, configurationrow.ForeignKeys...)
	}
	_spec.ScanValues = func(columns []string) ([]any, error) {
		return (*ConfigurationRow).scanValues(nil, columns)
	}
	_spec.Assign = func(columns []string, values []any) error {
		node := &ConfigurationRow{config: crq.config}
		nodes = append(nodes, node)
		node.Edges.loadedTypes = loadedTypes
		return node.assignValues(columns, values)
	}
	for i := range hooks {
		hooks[i](ctx, _spec)
	}
	if err := sqlgraph.QueryNodes(ctx, crq.driver, _spec); err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nodes, nil
	}
	if query := crq.withParent; query != nil {
		if err := crq.loadParent(ctx, query, nodes, nil,
			func(n *ConfigurationRow, e *Configuration) { n.Edges.Parent = e }); err != nil {
			return nil, err
		}
	}
	return nodes, nil
}

func (crq *ConfigurationRowQuery) loadParent(ctx context.Context, query *ConfigurationQuery, nodes []*ConfigurationRow, init func(*ConfigurationRow), assign func(*ConfigurationRow, *Configuration)) error {
	ids := make([]key.ConfigurationKey, 0, len(nodes))
	nodeids := make(map[key.ConfigurationKey][]*ConfigurationRow)
	for i := range nodes {
		if nodes[i].configuration_row_parent == nil {
			continue
		}
		fk := *nodes[i].configuration_row_parent
		if _, ok := nodeids[fk]; !ok {
			ids = append(ids, fk)
		}
		nodeids[fk] = append(nodeids[fk], nodes[i])
	}
	if len(ids) == 0 {
		return nil
	}
	query.Where(configuration.IDIn(ids...))
	neighbors, err := query.All(ctx)
	if err != nil {
		return err
	}
	for _, n := range neighbors {
		nodes, ok := nodeids[n.ID]
		if !ok {
			return fmt.Errorf(`unexpected foreign-key "configuration_row_parent" returned %v`, n.ID)
		}
		for i := range nodes {
			assign(nodes[i], n)
		}
	}
	return nil
}

func (crq *ConfigurationRowQuery) sqlCount(ctx context.Context) (int, error) {
	_spec := crq.querySpec()
	_spec.Node.Columns = crq.ctx.Fields
	if len(crq.ctx.Fields) > 0 {
		_spec.Unique = crq.ctx.Unique != nil && *crq.ctx.Unique
	}
	return sqlgraph.CountNodes(ctx, crq.driver, _spec)
}

func (crq *ConfigurationRowQuery) querySpec() *sqlgraph.QuerySpec {
	_spec := sqlgraph.NewQuerySpec(configurationrow.Table, configurationrow.Columns, sqlgraph.NewFieldSpec(configurationrow.FieldID, field.TypeString))
	_spec.From = crq.sql
	if unique := crq.ctx.Unique; unique != nil {
		_spec.Unique = *unique
	} else if crq.path != nil {
		_spec.Unique = true
	}
	if fields := crq.ctx.Fields; len(fields) > 0 {
		_spec.Node.Columns = make([]string, 0, len(fields))
		_spec.Node.Columns = append(_spec.Node.Columns, configurationrow.FieldID)
		for i := range fields {
			if fields[i] != configurationrow.FieldID {
				_spec.Node.Columns = append(_spec.Node.Columns, fields[i])
			}
		}
	}
	if ps := crq.predicates; len(ps) > 0 {
		_spec.Predicate = func(selector *sql.Selector) {
			for i := range ps {
				ps[i](selector)
			}
		}
	}
	if limit := crq.ctx.Limit; limit != nil {
		_spec.Limit = *limit
	}
	if offset := crq.ctx.Offset; offset != nil {
		_spec.Offset = *offset
	}
	if ps := crq.order; len(ps) > 0 {
		_spec.Order = func(selector *sql.Selector) {
			for i := range ps {
				ps[i](selector)
			}
		}
	}
	return _spec
}

func (crq *ConfigurationRowQuery) sqlQuery(ctx context.Context) *sql.Selector {
	builder := sql.Dialect(crq.driver.Dialect())
	t1 := builder.Table(configurationrow.Table)
	columns := crq.ctx.Fields
	if len(columns) == 0 {
		columns = configurationrow.Columns
	}
	selector := builder.Select(t1.Columns(columns...)...).From(t1)
	if crq.sql != nil {
		selector = crq.sql
		selector.Select(selector.Columns(columns...)...)
	}
	if crq.ctx.Unique != nil && *crq.ctx.Unique {
		selector.Distinct()
	}
	for _, p := range crq.predicates {
		p(selector)
	}
	for _, p := range crq.order {
		p(selector)
	}
	if offset := crq.ctx.Offset; offset != nil {
		// limit is mandatory for offset clause. We start
		// with default value, and override it below if needed.
		selector.Offset(*offset).Limit(math.MaxInt32)
	}
	if limit := crq.ctx.Limit; limit != nil {
		selector.Limit(*limit)
	}
	return selector
}

// ConfigurationRowGroupBy is the group-by builder for ConfigurationRow entities.
type ConfigurationRowGroupBy struct {
	selector
	build *ConfigurationRowQuery
}

// Aggregate adds the given aggregation functions to the group-by query.
func (crgb *ConfigurationRowGroupBy) Aggregate(fns ...AggregateFunc) *ConfigurationRowGroupBy {
	crgb.fns = append(crgb.fns, fns...)
	return crgb
}

// Scan applies the selector query and scans the result into the given value.
func (crgb *ConfigurationRowGroupBy) Scan(ctx context.Context, v any) error {
	ctx = setContextOp(ctx, crgb.build.ctx, ent.OpQueryGroupBy)
	if err := crgb.build.prepareQuery(ctx); err != nil {
		return err
	}
	return scanWithInterceptors[*ConfigurationRowQuery, *ConfigurationRowGroupBy](ctx, crgb.build, crgb, crgb.build.inters, v)
}

func (crgb *ConfigurationRowGroupBy) sqlScan(ctx context.Context, root *ConfigurationRowQuery, v any) error {
	selector := root.sqlQuery(ctx).Select()
	aggregation := make([]string, 0, len(crgb.fns))
	for _, fn := range crgb.fns {
		aggregation = append(aggregation, fn(selector))
	}
	if len(selector.SelectedColumns()) == 0 {
		columns := make([]string, 0, len(*crgb.flds)+len(crgb.fns))
		for _, f := range *crgb.flds {
			columns = append(columns, selector.C(f))
		}
		columns = append(columns, aggregation...)
		selector.Select(columns...)
	}
	selector.GroupBy(selector.Columns(*crgb.flds...)...)
	if err := selector.Err(); err != nil {
		return err
	}
	rows := &sql.Rows{}
	query, args := selector.Query()
	if err := crgb.build.driver.Query(ctx, query, args, rows); err != nil {
		return err
	}
	defer rows.Close()
	return sql.ScanSlice(rows, v)
}

// ConfigurationRowSelect is the builder for selecting fields of ConfigurationRow entities.
type ConfigurationRowSelect struct {
	*ConfigurationRowQuery
	selector
}

// Aggregate adds the given aggregation functions to the selector query.
func (crs *ConfigurationRowSelect) Aggregate(fns ...AggregateFunc) *ConfigurationRowSelect {
	crs.fns = append(crs.fns, fns...)
	return crs
}

// Scan applies the selector query and scans the result into the given value.
func (crs *ConfigurationRowSelect) Scan(ctx context.Context, v any) error {
	ctx = setContextOp(ctx, crs.ctx, ent.OpQuerySelect)
	if err := crs.prepareQuery(ctx); err != nil {
		return err
	}
	return scanWithInterceptors[*ConfigurationRowQuery, *ConfigurationRowSelect](ctx, crs.ConfigurationRowQuery, crs, crs.inters, v)
}

func (crs *ConfigurationRowSelect) sqlScan(ctx context.Context, root *ConfigurationRowQuery, v any) error {
	selector := root.sqlQuery(ctx)
	aggregation := make([]string, 0, len(crs.fns))
	for _, fn := range crs.fns {
		aggregation = append(aggregation, fn(selector))
	}
	switch n := len(*crs.selector.flds); {
	case n == 0 && len(aggregation) > 0:
		selector.Select(aggregation...)
	case n != 0 && len(aggregation) > 0:
		selector.AppendSelect(aggregation...)
	}
	rows := &sql.Rows{}
	query, args := selector.Query()
	if err := crs.driver.Query(ctx, query, args, rows); err != nil {
		return err
	}
	defer rows.Close()
	return sql.ScanSlice(rows, v)
}
