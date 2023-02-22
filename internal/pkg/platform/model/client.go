// Code generated by ent, DO NOT EDIT.

package model

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/keboola/keboola-as-code/internal/pkg/platform/model/key"
	"github.com/keboola/keboola-as-code/internal/pkg/platform/model/migrate"

	"github.com/keboola/keboola-as-code/internal/pkg/platform/model/branch"
	"github.com/keboola/keboola-as-code/internal/pkg/platform/model/configuration"
	"github.com/keboola/keboola-as-code/internal/pkg/platform/model/configurationrow"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"
	"entgo.io/ent/dialect/sql/sqlgraph"
)

// Client is the client that holds all ent builders.
type Client struct {
	config
	// Schema is the client for creating, migrating and dropping schema.
	Schema *migrate.Schema
	// Branch is the client for interacting with the Branch builders.
	Branch *BranchClient
	// Configuration is the client for interacting with the Configuration builders.
	Configuration *ConfigurationClient
	// ConfigurationRow is the client for interacting with the ConfigurationRow builders.
	ConfigurationRow *ConfigurationRowClient
}

// NewClient creates a new client configured with the given options.
func NewClient(opts ...Option) *Client {
	cfg := config{log: log.Println, hooks: &hooks{}, inters: &inters{}}
	cfg.options(opts...)
	client := &Client{config: cfg}
	client.init()
	return client
}

func (c *Client) init() {
	c.Schema = migrate.NewSchema(c.driver)
	c.Branch = NewBranchClient(c.config)
	c.Configuration = NewConfigurationClient(c.config)
	c.ConfigurationRow = NewConfigurationRowClient(c.config)
}

// Open opens a database/sql.DB specified by the driver name and
// the data source name, and returns a new client attached to it.
// Optional parameters can be added for configuring the client.
func Open(driverName, dataSourceName string, options ...Option) (*Client, error) {
	switch driverName {
	case dialect.MySQL, dialect.Postgres, dialect.SQLite:
		drv, err := sql.Open(driverName, dataSourceName)
		if err != nil {
			return nil, err
		}
		return NewClient(append(options, Driver(drv))...), nil
	default:
		return nil, fmt.Errorf("unsupported driver: %q", driverName)
	}
}

// Tx returns a new transactional client. The provided context
// is used until the transaction is committed or rolled back.
func (c *Client) Tx(ctx context.Context) (*Tx, error) {
	if _, ok := c.driver.(*txDriver); ok {
		return nil, errors.New("model: cannot start a transaction within a transaction")
	}
	tx, err := newTx(ctx, c.driver)
	if err != nil {
		return nil, fmt.Errorf("model: starting a transaction: %w", err)
	}
	cfg := c.config
	cfg.driver = tx
	return &Tx{
		ctx:              ctx,
		config:           cfg,
		Branch:           NewBranchClient(cfg),
		Configuration:    NewConfigurationClient(cfg),
		ConfigurationRow: NewConfigurationRowClient(cfg),
	}, nil
}

// BeginTx returns a transactional client with specified options.
func (c *Client) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	if _, ok := c.driver.(*txDriver); ok {
		return nil, errors.New("ent: cannot start a transaction within a transaction")
	}
	tx, err := c.driver.(interface {
		BeginTx(context.Context, *sql.TxOptions) (dialect.Tx, error)
	}).BeginTx(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("ent: starting a transaction: %w", err)
	}
	cfg := c.config
	cfg.driver = &txDriver{tx: tx, drv: c.driver}
	return &Tx{
		ctx:              ctx,
		config:           cfg,
		Branch:           NewBranchClient(cfg),
		Configuration:    NewConfigurationClient(cfg),
		ConfigurationRow: NewConfigurationRowClient(cfg),
	}, nil
}

// Debug returns a new debug-client. It's used to get verbose logging on specific operations.
//
//	client.Debug().
//		Branch.
//		Query().
//		Count(ctx)
func (c *Client) Debug() *Client {
	if c.debug {
		return c
	}
	cfg := c.config
	cfg.driver = dialect.Debug(c.driver, c.log)
	client := &Client{config: cfg}
	client.init()
	return client
}

// Close closes the database connection and prevents new queries from starting.
func (c *Client) Close() error {
	return c.driver.Close()
}

// Use adds the mutation hooks to all the entity clients.
// In order to add hooks to a specific client, call: `client.Node.Use(...)`.
func (c *Client) Use(hooks ...Hook) {
	c.Branch.Use(hooks...)
	c.Configuration.Use(hooks...)
	c.ConfigurationRow.Use(hooks...)
}

// Intercept adds the query interceptors to all the entity clients.
// In order to add interceptors to a specific client, call: `client.Node.Intercept(...)`.
func (c *Client) Intercept(interceptors ...Interceptor) {
	c.Branch.Intercept(interceptors...)
	c.Configuration.Intercept(interceptors...)
	c.ConfigurationRow.Intercept(interceptors...)
}

// Mutate implements the ent.Mutator interface.
func (c *Client) Mutate(ctx context.Context, m Mutation) (Value, error) {
	switch m := m.(type) {
	case *BranchMutation:
		return c.Branch.mutate(ctx, m)
	case *ConfigurationMutation:
		return c.Configuration.mutate(ctx, m)
	case *ConfigurationRowMutation:
		return c.ConfigurationRow.mutate(ctx, m)
	default:
		return nil, fmt.Errorf("model: unknown mutation type %T", m)
	}
}

// BranchClient is a client for the Branch schema.
type BranchClient struct {
	config
}

// NewBranchClient returns a client for the Branch from the given config.
func NewBranchClient(c config) *BranchClient {
	return &BranchClient{config: c}
}

// Use adds a list of mutation hooks to the hooks stack.
// A call to `Use(f, g, h)` equals to `branch.Hooks(f(g(h())))`.
func (c *BranchClient) Use(hooks ...Hook) {
	c.hooks.Branch = append(c.hooks.Branch, hooks...)
}

// Use adds a list of query interceptors to the interceptors stack.
// A call to `Intercept(f, g, h)` equals to `branch.Intercept(f(g(h())))`.
func (c *BranchClient) Intercept(interceptors ...Interceptor) {
	c.inters.Branch = append(c.inters.Branch, interceptors...)
}

// Create returns a builder for creating a Branch entity.
func (c *BranchClient) Create() *BranchCreate {
	mutation := newBranchMutation(c.config, OpCreate)
	return &BranchCreate{config: c.config, hooks: c.Hooks(), mutation: mutation}
}

// CreateBulk returns a builder for creating a bulk of Branch entities.
func (c *BranchClient) CreateBulk(builders ...*BranchCreate) *BranchCreateBulk {
	return &BranchCreateBulk{config: c.config, builders: builders}
}

// Update returns an update builder for Branch.
func (c *BranchClient) Update() *BranchUpdate {
	mutation := newBranchMutation(c.config, OpUpdate)
	return &BranchUpdate{config: c.config, hooks: c.Hooks(), mutation: mutation}
}

// UpdateOne returns an update builder for the given entity.
func (c *BranchClient) UpdateOne(b *Branch) *BranchUpdateOne {
	mutation := newBranchMutation(c.config, OpUpdateOne, withBranch(b))
	return &BranchUpdateOne{config: c.config, hooks: c.Hooks(), mutation: mutation}
}

// UpdateOneID returns an update builder for the given id.
func (c *BranchClient) UpdateOneID(id key.BranchKey) *BranchUpdateOne {
	mutation := newBranchMutation(c.config, OpUpdateOne, withBranchID(id))
	return &BranchUpdateOne{config: c.config, hooks: c.Hooks(), mutation: mutation}
}

// Delete returns a delete builder for Branch.
func (c *BranchClient) Delete() *BranchDelete {
	mutation := newBranchMutation(c.config, OpDelete)
	return &BranchDelete{config: c.config, hooks: c.Hooks(), mutation: mutation}
}

// DeleteOne returns a builder for deleting the given entity.
func (c *BranchClient) DeleteOne(b *Branch) *BranchDeleteOne {
	return c.DeleteOneID(b.ID)
}

// DeleteOneID returns a builder for deleting the given entity by its id.
func (c *BranchClient) DeleteOneID(id key.BranchKey) *BranchDeleteOne {
	builder := c.Delete().Where(branch.ID(id))
	builder.mutation.id = &id
	builder.mutation.op = OpDeleteOne
	return &BranchDeleteOne{builder}
}

// Query returns a query builder for Branch.
func (c *BranchClient) Query() *BranchQuery {
	return &BranchQuery{
		config: c.config,
		ctx:    &QueryContext{Type: TypeBranch},
		inters: c.Interceptors(),
	}
}

// Get returns a Branch entity by its id.
func (c *BranchClient) Get(ctx context.Context, id key.BranchKey) (*Branch, error) {
	return c.Query().Where(branch.ID(id)).Only(ctx)
}

// GetX is like Get, but panics if an error occurs.
func (c *BranchClient) GetX(ctx context.Context, id key.BranchKey) *Branch {
	obj, err := c.Get(ctx, id)
	if err != nil {
		panic(err)
	}
	return obj
}

// QueryConfigurations queries the configurations edge of a Branch.
func (c *BranchClient) QueryConfigurations(b *Branch) *ConfigurationQuery {
	query := (&ConfigurationClient{config: c.config}).Query()
	query.path = func(context.Context) (fromV *sql.Selector, _ error) {
		id := b.ID
		step := sqlgraph.NewStep(
			sqlgraph.From(branch.Table, branch.FieldID, id),
			sqlgraph.To(configuration.Table, configuration.FieldID),
			sqlgraph.Edge(sqlgraph.O2M, true, branch.ConfigurationsTable, branch.ConfigurationsColumn),
		)
		fromV = sqlgraph.Neighbors(b.driver.Dialect(), step)
		return fromV, nil
	}
	return query
}

// Hooks returns the client hooks.
func (c *BranchClient) Hooks() []Hook {
	return c.hooks.Branch
}

// Interceptors returns the client interceptors.
func (c *BranchClient) Interceptors() []Interceptor {
	return c.inters.Branch
}

func (c *BranchClient) mutate(ctx context.Context, m *BranchMutation) (Value, error) {
	switch m.Op() {
	case OpCreate:
		return (&BranchCreate{config: c.config, hooks: c.Hooks(), mutation: m}).Save(ctx)
	case OpUpdate:
		return (&BranchUpdate{config: c.config, hooks: c.Hooks(), mutation: m}).Save(ctx)
	case OpUpdateOne:
		return (&BranchUpdateOne{config: c.config, hooks: c.Hooks(), mutation: m}).Save(ctx)
	case OpDelete, OpDeleteOne:
		return (&BranchDelete{config: c.config, hooks: c.Hooks(), mutation: m}).Exec(ctx)
	default:
		return nil, fmt.Errorf("model: unknown Branch mutation op: %q", m.Op())
	}
}

// ConfigurationClient is a client for the Configuration schema.
type ConfigurationClient struct {
	config
}

// NewConfigurationClient returns a client for the Configuration from the given config.
func NewConfigurationClient(c config) *ConfigurationClient {
	return &ConfigurationClient{config: c}
}

// Use adds a list of mutation hooks to the hooks stack.
// A call to `Use(f, g, h)` equals to `configuration.Hooks(f(g(h())))`.
func (c *ConfigurationClient) Use(hooks ...Hook) {
	c.hooks.Configuration = append(c.hooks.Configuration, hooks...)
}

// Use adds a list of query interceptors to the interceptors stack.
// A call to `Intercept(f, g, h)` equals to `configuration.Intercept(f(g(h())))`.
func (c *ConfigurationClient) Intercept(interceptors ...Interceptor) {
	c.inters.Configuration = append(c.inters.Configuration, interceptors...)
}

// Create returns a builder for creating a Configuration entity.
func (c *ConfigurationClient) Create() *ConfigurationCreate {
	mutation := newConfigurationMutation(c.config, OpCreate)
	return &ConfigurationCreate{config: c.config, hooks: c.Hooks(), mutation: mutation}
}

// CreateBulk returns a builder for creating a bulk of Configuration entities.
func (c *ConfigurationClient) CreateBulk(builders ...*ConfigurationCreate) *ConfigurationCreateBulk {
	return &ConfigurationCreateBulk{config: c.config, builders: builders}
}

// Update returns an update builder for Configuration.
func (c *ConfigurationClient) Update() *ConfigurationUpdate {
	mutation := newConfigurationMutation(c.config, OpUpdate)
	return &ConfigurationUpdate{config: c.config, hooks: c.Hooks(), mutation: mutation}
}

// UpdateOne returns an update builder for the given entity.
func (c *ConfigurationClient) UpdateOne(co *Configuration) *ConfigurationUpdateOne {
	mutation := newConfigurationMutation(c.config, OpUpdateOne, withConfiguration(co))
	return &ConfigurationUpdateOne{config: c.config, hooks: c.Hooks(), mutation: mutation}
}

// UpdateOneID returns an update builder for the given id.
func (c *ConfigurationClient) UpdateOneID(id key.ConfigurationKey) *ConfigurationUpdateOne {
	mutation := newConfigurationMutation(c.config, OpUpdateOne, withConfigurationID(id))
	return &ConfigurationUpdateOne{config: c.config, hooks: c.Hooks(), mutation: mutation}
}

// Delete returns a delete builder for Configuration.
func (c *ConfigurationClient) Delete() *ConfigurationDelete {
	mutation := newConfigurationMutation(c.config, OpDelete)
	return &ConfigurationDelete{config: c.config, hooks: c.Hooks(), mutation: mutation}
}

// DeleteOne returns a builder for deleting the given entity.
func (c *ConfigurationClient) DeleteOne(co *Configuration) *ConfigurationDeleteOne {
	return c.DeleteOneID(co.ID)
}

// DeleteOneID returns a builder for deleting the given entity by its id.
func (c *ConfigurationClient) DeleteOneID(id key.ConfigurationKey) *ConfigurationDeleteOne {
	builder := c.Delete().Where(configuration.ID(id))
	builder.mutation.id = &id
	builder.mutation.op = OpDeleteOne
	return &ConfigurationDeleteOne{builder}
}

// Query returns a query builder for Configuration.
func (c *ConfigurationClient) Query() *ConfigurationQuery {
	return &ConfigurationQuery{
		config: c.config,
		ctx:    &QueryContext{Type: TypeConfiguration},
		inters: c.Interceptors(),
	}
}

// Get returns a Configuration entity by its id.
func (c *ConfigurationClient) Get(ctx context.Context, id key.ConfigurationKey) (*Configuration, error) {
	return c.Query().Where(configuration.ID(id)).Only(ctx)
}

// GetX is like Get, but panics if an error occurs.
func (c *ConfigurationClient) GetX(ctx context.Context, id key.ConfigurationKey) *Configuration {
	obj, err := c.Get(ctx, id)
	if err != nil {
		panic(err)
	}
	return obj
}

// QueryParent queries the parent edge of a Configuration.
func (c *ConfigurationClient) QueryParent(co *Configuration) *BranchQuery {
	query := (&BranchClient{config: c.config}).Query()
	query.path = func(context.Context) (fromV *sql.Selector, _ error) {
		id := co.ID
		step := sqlgraph.NewStep(
			sqlgraph.From(configuration.Table, configuration.FieldID, id),
			sqlgraph.To(branch.Table, branch.FieldID),
			sqlgraph.Edge(sqlgraph.M2O, false, configuration.ParentTable, configuration.ParentColumn),
		)
		fromV = sqlgraph.Neighbors(co.driver.Dialect(), step)
		return fromV, nil
	}
	return query
}

// Hooks returns the client hooks.
func (c *ConfigurationClient) Hooks() []Hook {
	return c.hooks.Configuration
}

// Interceptors returns the client interceptors.
func (c *ConfigurationClient) Interceptors() []Interceptor {
	return c.inters.Configuration
}

func (c *ConfigurationClient) mutate(ctx context.Context, m *ConfigurationMutation) (Value, error) {
	switch m.Op() {
	case OpCreate:
		return (&ConfigurationCreate{config: c.config, hooks: c.Hooks(), mutation: m}).Save(ctx)
	case OpUpdate:
		return (&ConfigurationUpdate{config: c.config, hooks: c.Hooks(), mutation: m}).Save(ctx)
	case OpUpdateOne:
		return (&ConfigurationUpdateOne{config: c.config, hooks: c.Hooks(), mutation: m}).Save(ctx)
	case OpDelete, OpDeleteOne:
		return (&ConfigurationDelete{config: c.config, hooks: c.Hooks(), mutation: m}).Exec(ctx)
	default:
		return nil, fmt.Errorf("model: unknown Configuration mutation op: %q", m.Op())
	}
}

// ConfigurationRowClient is a client for the ConfigurationRow schema.
type ConfigurationRowClient struct {
	config
}

// NewConfigurationRowClient returns a client for the ConfigurationRow from the given config.
func NewConfigurationRowClient(c config) *ConfigurationRowClient {
	return &ConfigurationRowClient{config: c}
}

// Use adds a list of mutation hooks to the hooks stack.
// A call to `Use(f, g, h)` equals to `configurationrow.Hooks(f(g(h())))`.
func (c *ConfigurationRowClient) Use(hooks ...Hook) {
	c.hooks.ConfigurationRow = append(c.hooks.ConfigurationRow, hooks...)
}

// Use adds a list of query interceptors to the interceptors stack.
// A call to `Intercept(f, g, h)` equals to `configurationrow.Intercept(f(g(h())))`.
func (c *ConfigurationRowClient) Intercept(interceptors ...Interceptor) {
	c.inters.ConfigurationRow = append(c.inters.ConfigurationRow, interceptors...)
}

// Create returns a builder for creating a ConfigurationRow entity.
func (c *ConfigurationRowClient) Create() *ConfigurationRowCreate {
	mutation := newConfigurationRowMutation(c.config, OpCreate)
	return &ConfigurationRowCreate{config: c.config, hooks: c.Hooks(), mutation: mutation}
}

// CreateBulk returns a builder for creating a bulk of ConfigurationRow entities.
func (c *ConfigurationRowClient) CreateBulk(builders ...*ConfigurationRowCreate) *ConfigurationRowCreateBulk {
	return &ConfigurationRowCreateBulk{config: c.config, builders: builders}
}

// Update returns an update builder for ConfigurationRow.
func (c *ConfigurationRowClient) Update() *ConfigurationRowUpdate {
	mutation := newConfigurationRowMutation(c.config, OpUpdate)
	return &ConfigurationRowUpdate{config: c.config, hooks: c.Hooks(), mutation: mutation}
}

// UpdateOne returns an update builder for the given entity.
func (c *ConfigurationRowClient) UpdateOne(cr *ConfigurationRow) *ConfigurationRowUpdateOne {
	mutation := newConfigurationRowMutation(c.config, OpUpdateOne, withConfigurationRow(cr))
	return &ConfigurationRowUpdateOne{config: c.config, hooks: c.Hooks(), mutation: mutation}
}

// UpdateOneID returns an update builder for the given id.
func (c *ConfigurationRowClient) UpdateOneID(id key.ConfigurationRowKey) *ConfigurationRowUpdateOne {
	mutation := newConfigurationRowMutation(c.config, OpUpdateOne, withConfigurationRowID(id))
	return &ConfigurationRowUpdateOne{config: c.config, hooks: c.Hooks(), mutation: mutation}
}

// Delete returns a delete builder for ConfigurationRow.
func (c *ConfigurationRowClient) Delete() *ConfigurationRowDelete {
	mutation := newConfigurationRowMutation(c.config, OpDelete)
	return &ConfigurationRowDelete{config: c.config, hooks: c.Hooks(), mutation: mutation}
}

// DeleteOne returns a builder for deleting the given entity.
func (c *ConfigurationRowClient) DeleteOne(cr *ConfigurationRow) *ConfigurationRowDeleteOne {
	return c.DeleteOneID(cr.ID)
}

// DeleteOneID returns a builder for deleting the given entity by its id.
func (c *ConfigurationRowClient) DeleteOneID(id key.ConfigurationRowKey) *ConfigurationRowDeleteOne {
	builder := c.Delete().Where(configurationrow.ID(id))
	builder.mutation.id = &id
	builder.mutation.op = OpDeleteOne
	return &ConfigurationRowDeleteOne{builder}
}

// Query returns a query builder for ConfigurationRow.
func (c *ConfigurationRowClient) Query() *ConfigurationRowQuery {
	return &ConfigurationRowQuery{
		config: c.config,
		ctx:    &QueryContext{Type: TypeConfigurationRow},
		inters: c.Interceptors(),
	}
}

// Get returns a ConfigurationRow entity by its id.
func (c *ConfigurationRowClient) Get(ctx context.Context, id key.ConfigurationRowKey) (*ConfigurationRow, error) {
	return c.Query().Where(configurationrow.ID(id)).Only(ctx)
}

// GetX is like Get, but panics if an error occurs.
func (c *ConfigurationRowClient) GetX(ctx context.Context, id key.ConfigurationRowKey) *ConfigurationRow {
	obj, err := c.Get(ctx, id)
	if err != nil {
		panic(err)
	}
	return obj
}

// QueryParent queries the parent edge of a ConfigurationRow.
func (c *ConfigurationRowClient) QueryParent(cr *ConfigurationRow) *ConfigurationQuery {
	query := (&ConfigurationClient{config: c.config}).Query()
	query.path = func(context.Context) (fromV *sql.Selector, _ error) {
		id := cr.ID
		step := sqlgraph.NewStep(
			sqlgraph.From(configurationrow.Table, configurationrow.FieldID, id),
			sqlgraph.To(configuration.Table, configuration.FieldID),
			sqlgraph.Edge(sqlgraph.M2O, false, configurationrow.ParentTable, configurationrow.ParentColumn),
		)
		fromV = sqlgraph.Neighbors(cr.driver.Dialect(), step)
		return fromV, nil
	}
	return query
}

// Hooks returns the client hooks.
func (c *ConfigurationRowClient) Hooks() []Hook {
	return c.hooks.ConfigurationRow
}

// Interceptors returns the client interceptors.
func (c *ConfigurationRowClient) Interceptors() []Interceptor {
	return c.inters.ConfigurationRow
}

func (c *ConfigurationRowClient) mutate(ctx context.Context, m *ConfigurationRowMutation) (Value, error) {
	switch m.Op() {
	case OpCreate:
		return (&ConfigurationRowCreate{config: c.config, hooks: c.Hooks(), mutation: m}).Save(ctx)
	case OpUpdate:
		return (&ConfigurationRowUpdate{config: c.config, hooks: c.Hooks(), mutation: m}).Save(ctx)
	case OpUpdateOne:
		return (&ConfigurationRowUpdateOne{config: c.config, hooks: c.Hooks(), mutation: m}).Save(ctx)
	case OpDelete, OpDeleteOne:
		return (&ConfigurationRowDelete{config: c.config, hooks: c.Hooks(), mutation: m}).Exec(ctx)
	default:
		return nil, fmt.Errorf("model: unknown ConfigurationRow mutation op: %q", m.Op())
	}
}
