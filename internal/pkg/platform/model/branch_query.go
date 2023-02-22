// Code generated by ent, DO NOT EDIT.

package model

import (
	"context"
	"database/sql/driver"
	"fmt"
	"math"

	"entgo.io/ent/dialect/sql"
	"entgo.io/ent/dialect/sql/sqlgraph"
	"entgo.io/ent/schema/field"
	"github.com/keboola/keboola-as-code/internal/pkg/platform/model/branch"
	"github.com/keboola/keboola-as-code/internal/pkg/platform/model/configuration"
	"github.com/keboola/keboola-as-code/internal/pkg/platform/model/key"
	"github.com/keboola/keboola-as-code/internal/pkg/platform/model/predicate"
)

// BranchQuery is the builder for querying Branch entities.
type BranchQuery struct {
	config
	ctx                *QueryContext
	order              []OrderFunc
	inters             []Interceptor
	predicates         []predicate.Branch
	withConfigurations *ConfigurationQuery
	// intermediate query (i.e. traversal path).
	sql  *sql.Selector
	path func(context.Context) (*sql.Selector, error)
}

// Where adds a new predicate for the BranchQuery builder.
func (bq *BranchQuery) Where(ps ...predicate.Branch) *BranchQuery {
	bq.predicates = append(bq.predicates, ps...)
	return bq
}

// Limit the number of records to be returned by this query.
func (bq *BranchQuery) Limit(limit int) *BranchQuery {
	bq.ctx.Limit = &limit
	return bq
}

// Offset to start from.
func (bq *BranchQuery) Offset(offset int) *BranchQuery {
	bq.ctx.Offset = &offset
	return bq
}

// Unique configures the query builder to filter duplicate records on query.
// By default, unique is set to true, and can be disabled using this method.
func (bq *BranchQuery) Unique(unique bool) *BranchQuery {
	bq.ctx.Unique = &unique
	return bq
}

// Order specifies how the records should be ordered.
func (bq *BranchQuery) Order(o ...OrderFunc) *BranchQuery {
	bq.order = append(bq.order, o...)
	return bq
}

// QueryConfigurations chains the current query on the "configurations" edge.
func (bq *BranchQuery) QueryConfigurations() *ConfigurationQuery {
	query := (&ConfigurationClient{config: bq.config}).Query()
	query.path = func(ctx context.Context) (fromU *sql.Selector, err error) {
		if err := bq.prepareQuery(ctx); err != nil {
			return nil, err
		}
		selector := bq.sqlQuery(ctx)
		if err := selector.Err(); err != nil {
			return nil, err
		}
		step := sqlgraph.NewStep(
			sqlgraph.From(branch.Table, branch.FieldID, selector),
			sqlgraph.To(configuration.Table, configuration.FieldID),
			sqlgraph.Edge(sqlgraph.O2M, true, branch.ConfigurationsTable, branch.ConfigurationsColumn),
		)
		fromU = sqlgraph.SetNeighbors(bq.driver.Dialect(), step)
		return fromU, nil
	}
	return query
}

// First returns the first Branch entity from the query.
// Returns a *NotFoundError when no Branch was found.
func (bq *BranchQuery) First(ctx context.Context) (*Branch, error) {
	nodes, err := bq.Limit(1).All(setContextOp(ctx, bq.ctx, "First"))
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, &NotFoundError{branch.Label}
	}
	return nodes[0], nil
}

// FirstX is like First, but panics if an error occurs.
func (bq *BranchQuery) FirstX(ctx context.Context) *Branch {
	node, err := bq.First(ctx)
	if err != nil && !IsNotFound(err) {
		panic(err)
	}
	return node
}

// FirstID returns the first Branch ID from the query.
// Returns a *NotFoundError when no Branch ID was found.
func (bq *BranchQuery) FirstID(ctx context.Context) (id key.BranchKey, err error) {
	var ids []key.BranchKey
	if ids, err = bq.Limit(1).IDs(setContextOp(ctx, bq.ctx, "FirstID")); err != nil {
		return
	}
	if len(ids) == 0 {
		err = &NotFoundError{branch.Label}
		return
	}
	return ids[0], nil
}

// FirstIDX is like FirstID, but panics if an error occurs.
func (bq *BranchQuery) FirstIDX(ctx context.Context) key.BranchKey {
	id, err := bq.FirstID(ctx)
	if err != nil && !IsNotFound(err) {
		panic(err)
	}
	return id
}

// Only returns a single Branch entity found by the query, ensuring it only returns one.
// Returns a *NotSingularError when more than one Branch entity is found.
// Returns a *NotFoundError when no Branch entities are found.
func (bq *BranchQuery) Only(ctx context.Context) (*Branch, error) {
	nodes, err := bq.Limit(2).All(setContextOp(ctx, bq.ctx, "Only"))
	if err != nil {
		return nil, err
	}
	switch len(nodes) {
	case 1:
		return nodes[0], nil
	case 0:
		return nil, &NotFoundError{branch.Label}
	default:
		return nil, &NotSingularError{branch.Label}
	}
}

// OnlyX is like Only, but panics if an error occurs.
func (bq *BranchQuery) OnlyX(ctx context.Context) *Branch {
	node, err := bq.Only(ctx)
	if err != nil {
		panic(err)
	}
	return node
}

// OnlyID is like Only, but returns the only Branch ID in the query.
// Returns a *NotSingularError when more than one Branch ID is found.
// Returns a *NotFoundError when no entities are found.
func (bq *BranchQuery) OnlyID(ctx context.Context) (id key.BranchKey, err error) {
	var ids []key.BranchKey
	if ids, err = bq.Limit(2).IDs(setContextOp(ctx, bq.ctx, "OnlyID")); err != nil {
		return
	}
	switch len(ids) {
	case 1:
		id = ids[0]
	case 0:
		err = &NotFoundError{branch.Label}
	default:
		err = &NotSingularError{branch.Label}
	}
	return
}

// OnlyIDX is like OnlyID, but panics if an error occurs.
func (bq *BranchQuery) OnlyIDX(ctx context.Context) key.BranchKey {
	id, err := bq.OnlyID(ctx)
	if err != nil {
		panic(err)
	}
	return id
}

// All executes the query and returns a list of Branches.
func (bq *BranchQuery) All(ctx context.Context) ([]*Branch, error) {
	ctx = setContextOp(ctx, bq.ctx, "All")
	if err := bq.prepareQuery(ctx); err != nil {
		return nil, err
	}
	qr := querierAll[[]*Branch, *BranchQuery]()
	return withInterceptors[[]*Branch](ctx, bq, qr, bq.inters)
}

// AllX is like All, but panics if an error occurs.
func (bq *BranchQuery) AllX(ctx context.Context) []*Branch {
	nodes, err := bq.All(ctx)
	if err != nil {
		panic(err)
	}
	return nodes
}

// IDs executes the query and returns a list of Branch IDs.
func (bq *BranchQuery) IDs(ctx context.Context) ([]key.BranchKey, error) {
	var ids []key.BranchKey
	ctx = setContextOp(ctx, bq.ctx, "IDs")
	if err := bq.Select(branch.FieldID).Scan(ctx, &ids); err != nil {
		return nil, err
	}
	return ids, nil
}

// IDsX is like IDs, but panics if an error occurs.
func (bq *BranchQuery) IDsX(ctx context.Context) []key.BranchKey {
	ids, err := bq.IDs(ctx)
	if err != nil {
		panic(err)
	}
	return ids
}

// Count returns the count of the given query.
func (bq *BranchQuery) Count(ctx context.Context) (int, error) {
	ctx = setContextOp(ctx, bq.ctx, "Count")
	if err := bq.prepareQuery(ctx); err != nil {
		return 0, err
	}
	return withInterceptors[int](ctx, bq, querierCount[*BranchQuery](), bq.inters)
}

// CountX is like Count, but panics if an error occurs.
func (bq *BranchQuery) CountX(ctx context.Context) int {
	count, err := bq.Count(ctx)
	if err != nil {
		panic(err)
	}
	return count
}

// Exist returns true if the query has elements in the graph.
func (bq *BranchQuery) Exist(ctx context.Context) (bool, error) {
	ctx = setContextOp(ctx, bq.ctx, "Exist")
	switch _, err := bq.FirstID(ctx); {
	case IsNotFound(err):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("model: check existence: %w", err)
	default:
		return true, nil
	}
}

// ExistX is like Exist, but panics if an error occurs.
func (bq *BranchQuery) ExistX(ctx context.Context) bool {
	exist, err := bq.Exist(ctx)
	if err != nil {
		panic(err)
	}
	return exist
}

// Clone returns a duplicate of the BranchQuery builder, including all associated steps. It can be
// used to prepare common query builders and use them differently after the clone is made.
func (bq *BranchQuery) Clone() *BranchQuery {
	if bq == nil {
		return nil
	}
	return &BranchQuery{
		config:             bq.config,
		ctx:                bq.ctx.Clone(),
		order:              append([]OrderFunc{}, bq.order...),
		inters:             append([]Interceptor{}, bq.inters...),
		predicates:         append([]predicate.Branch{}, bq.predicates...),
		withConfigurations: bq.withConfigurations.Clone(),
		// clone intermediate query.
		sql:  bq.sql.Clone(),
		path: bq.path,
	}
}

// WithConfigurations tells the query-builder to eager-load the nodes that are connected to
// the "configurations" edge. The optional arguments are used to configure the query builder of the edge.
func (bq *BranchQuery) WithConfigurations(opts ...func(*ConfigurationQuery)) *BranchQuery {
	query := (&ConfigurationClient{config: bq.config}).Query()
	for _, opt := range opts {
		opt(query)
	}
	bq.withConfigurations = query
	return bq
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
//	client.Branch.Query().
//		GroupBy(branch.FieldBranchID).
//		Aggregate(model.Count()).
//		Scan(ctx, &v)
func (bq *BranchQuery) GroupBy(field string, fields ...string) *BranchGroupBy {
	bq.ctx.Fields = append([]string{field}, fields...)
	grbuild := &BranchGroupBy{build: bq}
	grbuild.flds = &bq.ctx.Fields
	grbuild.label = branch.Label
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
//	client.Branch.Query().
//		Select(branch.FieldBranchID).
//		Scan(ctx, &v)
func (bq *BranchQuery) Select(fields ...string) *BranchSelect {
	bq.ctx.Fields = append(bq.ctx.Fields, fields...)
	sbuild := &BranchSelect{BranchQuery: bq}
	sbuild.label = branch.Label
	sbuild.flds, sbuild.scan = &bq.ctx.Fields, sbuild.Scan
	return sbuild
}

// Aggregate returns a BranchSelect configured with the given aggregations.
func (bq *BranchQuery) Aggregate(fns ...AggregateFunc) *BranchSelect {
	return bq.Select().Aggregate(fns...)
}

func (bq *BranchQuery) prepareQuery(ctx context.Context) error {
	for _, inter := range bq.inters {
		if inter == nil {
			return fmt.Errorf("model: uninitialized interceptor (forgotten import model/runtime?)")
		}
		if trv, ok := inter.(Traverser); ok {
			if err := trv.Traverse(ctx, bq); err != nil {
				return err
			}
		}
	}
	for _, f := range bq.ctx.Fields {
		if !branch.ValidColumn(f) {
			return &ValidationError{Name: f, err: fmt.Errorf("model: invalid field %q for query", f)}
		}
	}
	if bq.path != nil {
		prev, err := bq.path(ctx)
		if err != nil {
			return err
		}
		bq.sql = prev
	}
	return nil
}

func (bq *BranchQuery) sqlAll(ctx context.Context, hooks ...queryHook) ([]*Branch, error) {
	var (
		nodes       = []*Branch{}
		_spec       = bq.querySpec()
		loadedTypes = [1]bool{
			bq.withConfigurations != nil,
		}
	)
	_spec.ScanValues = func(columns []string) ([]any, error) {
		return (*Branch).scanValues(nil, columns)
	}
	_spec.Assign = func(columns []string, values []any) error {
		node := &Branch{config: bq.config}
		nodes = append(nodes, node)
		node.Edges.loadedTypes = loadedTypes
		return node.assignValues(columns, values)
	}
	for i := range hooks {
		hooks[i](ctx, _spec)
	}
	if err := sqlgraph.QueryNodes(ctx, bq.driver, _spec); err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nodes, nil
	}
	if query := bq.withConfigurations; query != nil {
		if err := bq.loadConfigurations(ctx, query, nodes,
			func(n *Branch) { n.Edges.Configurations = []*Configuration{} },
			func(n *Branch, e *Configuration) { n.Edges.Configurations = append(n.Edges.Configurations, e) }); err != nil {
			return nil, err
		}
	}
	return nodes, nil
}

func (bq *BranchQuery) loadConfigurations(ctx context.Context, query *ConfigurationQuery, nodes []*Branch, init func(*Branch), assign func(*Branch, *Configuration)) error {
	fks := make([]driver.Value, 0, len(nodes))
	nodeids := make(map[key.BranchKey]*Branch)
	for i := range nodes {
		fks = append(fks, nodes[i].ID)
		nodeids[nodes[i].ID] = nodes[i]
		if init != nil {
			init(nodes[i])
		}
	}
	query.withFKs = true
	query.Where(predicate.Configuration(func(s *sql.Selector) {
		s.Where(sql.InValues(branch.ConfigurationsColumn, fks...))
	}))
	neighbors, err := query.All(ctx)
	if err != nil {
		return err
	}
	for _, n := range neighbors {
		fk := n.configuration_parent
		if fk == nil {
			return fmt.Errorf(`foreign-key "configuration_parent" is nil for node %v`, n.ID)
		}
		node, ok := nodeids[*fk]
		if !ok {
			return fmt.Errorf(`unexpected foreign-key "configuration_parent" returned %v for node %v`, *fk, n.ID)
		}
		assign(node, n)
	}
	return nil
}

func (bq *BranchQuery) sqlCount(ctx context.Context) (int, error) {
	_spec := bq.querySpec()
	_spec.Node.Columns = bq.ctx.Fields
	if len(bq.ctx.Fields) > 0 {
		_spec.Unique = bq.ctx.Unique != nil && *bq.ctx.Unique
	}
	return sqlgraph.CountNodes(ctx, bq.driver, _spec)
}

func (bq *BranchQuery) querySpec() *sqlgraph.QuerySpec {
	_spec := &sqlgraph.QuerySpec{
		Node: &sqlgraph.NodeSpec{
			Table:   branch.Table,
			Columns: branch.Columns,
			ID: &sqlgraph.FieldSpec{
				Type:   field.TypeString,
				Column: branch.FieldID,
			},
		},
		From:   bq.sql,
		Unique: true,
	}
	if unique := bq.ctx.Unique; unique != nil {
		_spec.Unique = *unique
	}
	if fields := bq.ctx.Fields; len(fields) > 0 {
		_spec.Node.Columns = make([]string, 0, len(fields))
		_spec.Node.Columns = append(_spec.Node.Columns, branch.FieldID)
		for i := range fields {
			if fields[i] != branch.FieldID {
				_spec.Node.Columns = append(_spec.Node.Columns, fields[i])
			}
		}
	}
	if ps := bq.predicates; len(ps) > 0 {
		_spec.Predicate = func(selector *sql.Selector) {
			for i := range ps {
				ps[i](selector)
			}
		}
	}
	if limit := bq.ctx.Limit; limit != nil {
		_spec.Limit = *limit
	}
	if offset := bq.ctx.Offset; offset != nil {
		_spec.Offset = *offset
	}
	if ps := bq.order; len(ps) > 0 {
		_spec.Order = func(selector *sql.Selector) {
			for i := range ps {
				ps[i](selector)
			}
		}
	}
	return _spec
}

func (bq *BranchQuery) sqlQuery(ctx context.Context) *sql.Selector {
	builder := sql.Dialect(bq.driver.Dialect())
	t1 := builder.Table(branch.Table)
	columns := bq.ctx.Fields
	if len(columns) == 0 {
		columns = branch.Columns
	}
	selector := builder.Select(t1.Columns(columns...)...).From(t1)
	if bq.sql != nil {
		selector = bq.sql
		selector.Select(selector.Columns(columns...)...)
	}
	if bq.ctx.Unique != nil && *bq.ctx.Unique {
		selector.Distinct()
	}
	for _, p := range bq.predicates {
		p(selector)
	}
	for _, p := range bq.order {
		p(selector)
	}
	if offset := bq.ctx.Offset; offset != nil {
		// limit is mandatory for offset clause. We start
		// with default value, and override it below if needed.
		selector.Offset(*offset).Limit(math.MaxInt32)
	}
	if limit := bq.ctx.Limit; limit != nil {
		selector.Limit(*limit)
	}
	return selector
}

// BranchGroupBy is the group-by builder for Branch entities.
type BranchGroupBy struct {
	selector
	build *BranchQuery
}

// Aggregate adds the given aggregation functions to the group-by query.
func (bgb *BranchGroupBy) Aggregate(fns ...AggregateFunc) *BranchGroupBy {
	bgb.fns = append(bgb.fns, fns...)
	return bgb
}

// Scan applies the selector query and scans the result into the given value.
func (bgb *BranchGroupBy) Scan(ctx context.Context, v any) error {
	ctx = setContextOp(ctx, bgb.build.ctx, "GroupBy")
	if err := bgb.build.prepareQuery(ctx); err != nil {
		return err
	}
	return scanWithInterceptors[*BranchQuery, *BranchGroupBy](ctx, bgb.build, bgb, bgb.build.inters, v)
}

func (bgb *BranchGroupBy) sqlScan(ctx context.Context, root *BranchQuery, v any) error {
	selector := root.sqlQuery(ctx).Select()
	aggregation := make([]string, 0, len(bgb.fns))
	for _, fn := range bgb.fns {
		aggregation = append(aggregation, fn(selector))
	}
	if len(selector.SelectedColumns()) == 0 {
		columns := make([]string, 0, len(*bgb.flds)+len(bgb.fns))
		for _, f := range *bgb.flds {
			columns = append(columns, selector.C(f))
		}
		columns = append(columns, aggregation...)
		selector.Select(columns...)
	}
	selector.GroupBy(selector.Columns(*bgb.flds...)...)
	if err := selector.Err(); err != nil {
		return err
	}
	rows := &sql.Rows{}
	query, args := selector.Query()
	if err := bgb.build.driver.Query(ctx, query, args, rows); err != nil {
		return err
	}
	defer rows.Close()
	return sql.ScanSlice(rows, v)
}

// BranchSelect is the builder for selecting fields of Branch entities.
type BranchSelect struct {
	*BranchQuery
	selector
}

// Aggregate adds the given aggregation functions to the selector query.
func (bs *BranchSelect) Aggregate(fns ...AggregateFunc) *BranchSelect {
	bs.fns = append(bs.fns, fns...)
	return bs
}

// Scan applies the selector query and scans the result into the given value.
func (bs *BranchSelect) Scan(ctx context.Context, v any) error {
	ctx = setContextOp(ctx, bs.ctx, "Select")
	if err := bs.prepareQuery(ctx); err != nil {
		return err
	}
	return scanWithInterceptors[*BranchQuery, *BranchSelect](ctx, bs.BranchQuery, bs, bs.inters, v)
}

func (bs *BranchSelect) sqlScan(ctx context.Context, root *BranchQuery, v any) error {
	selector := root.sqlQuery(ctx)
	aggregation := make([]string, 0, len(bs.fns))
	for _, fn := range bs.fns {
		aggregation = append(aggregation, fn(selector))
	}
	switch n := len(*bs.selector.flds); {
	case n == 0 && len(aggregation) > 0:
		selector.Select(aggregation...)
	case n != 0 && len(aggregation) > 0:
		selector.AppendSelect(aggregation...)
	}
	rows := &sql.Rows{}
	query, args := selector.Query()
	if err := bs.driver.Query(ctx, query, args, rows); err != nil {
		return err
	}
	defer rows.Close()
	return sql.ScanSlice(rows, v)
}
