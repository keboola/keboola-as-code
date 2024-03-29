// Code generated by ent, DO NOT EDIT.

package configurationrow

import (
	"entgo.io/ent/dialect/sql"
	"entgo.io/ent/dialect/sql/sqlgraph"
)

const (
	// Label holds the string label denoting the configurationrow type in the database.
	Label = "configuration_row"
	// FieldID holds the string denoting the id field in the database.
	FieldID = "id"
	// FieldBranchID holds the string denoting the branchid field in the database.
	FieldBranchID = "branch_id"
	// FieldComponentID holds the string denoting the componentid field in the database.
	FieldComponentID = "component_id"
	// FieldConfigID holds the string denoting the configid field in the database.
	FieldConfigID = "config_id"
	// FieldRowID holds the string denoting the rowid field in the database.
	FieldRowID = "row_id"
	// FieldName holds the string denoting the name field in the database.
	FieldName = "name"
	// FieldDescription holds the string denoting the description field in the database.
	FieldDescription = "description"
	// FieldIsDisabled holds the string denoting the isdisabled field in the database.
	FieldIsDisabled = "is_disabled"
	// FieldContent holds the string denoting the content field in the database.
	FieldContent = "content"
	// EdgeParent holds the string denoting the parent edge name in mutations.
	EdgeParent = "parent"
	// Table holds the table name of the configurationrow in the database.
	Table = "configuration_rows"
	// ParentTable is the table that holds the parent relation/edge.
	ParentTable = "configuration_rows"
	// ParentInverseTable is the table name for the Configuration entity.
	// It exists in this package in order to avoid circular dependency with the "configuration" package.
	ParentInverseTable = "configurations"
	// ParentColumn is the table column denoting the parent relation/edge.
	ParentColumn = "configuration_row_parent"
)

// Columns holds all SQL columns for configurationrow fields.
var Columns = []string{
	FieldID,
	FieldBranchID,
	FieldComponentID,
	FieldConfigID,
	FieldRowID,
	FieldName,
	FieldDescription,
	FieldIsDisabled,
	FieldContent,
}

// ForeignKeys holds the SQL foreign-keys that are owned by the "configuration_rows"
// table and are not defined as standalone fields in the schema.
var ForeignKeys = []string{
	"configuration_row_parent",
}

// ValidColumn reports if the column name is valid (part of the table columns).
func ValidColumn(column string) bool {
	for i := range Columns {
		if column == Columns[i] {
			return true
		}
	}
	for i := range ForeignKeys {
		if column == ForeignKeys[i] {
			return true
		}
	}
	return false
}

var (
	// BranchIDValidator is a validator for the "branchID" field. It is called by the builders before save.
	BranchIDValidator func(int) error
	// ComponentIDValidator is a validator for the "componentID" field. It is called by the builders before save.
	ComponentIDValidator func(string) error
	// ConfigIDValidator is a validator for the "configID" field. It is called by the builders before save.
	ConfigIDValidator func(string) error
	// RowIDValidator is a validator for the "rowID" field. It is called by the builders before save.
	RowIDValidator func(string) error
	// NameValidator is a validator for the "name" field. It is called by the builders before save.
	NameValidator func(string) error
	// DefaultIsDisabled holds the default value on creation for the "isDisabled" field.
	DefaultIsDisabled bool
	// IDValidator is a validator for the "id" field. It is called by the builders before save.
	IDValidator func(string) error
)

// OrderOption defines the ordering options for the ConfigurationRow queries.
type OrderOption func(*sql.Selector)

// ByID orders the results by the id field.
func ByID(opts ...sql.OrderTermOption) OrderOption {
	return sql.OrderByField(FieldID, opts...).ToFunc()
}

// ByBranchID orders the results by the branchID field.
func ByBranchID(opts ...sql.OrderTermOption) OrderOption {
	return sql.OrderByField(FieldBranchID, opts...).ToFunc()
}

// ByComponentID orders the results by the componentID field.
func ByComponentID(opts ...sql.OrderTermOption) OrderOption {
	return sql.OrderByField(FieldComponentID, opts...).ToFunc()
}

// ByConfigID orders the results by the configID field.
func ByConfigID(opts ...sql.OrderTermOption) OrderOption {
	return sql.OrderByField(FieldConfigID, opts...).ToFunc()
}

// ByRowID orders the results by the rowID field.
func ByRowID(opts ...sql.OrderTermOption) OrderOption {
	return sql.OrderByField(FieldRowID, opts...).ToFunc()
}

// ByName orders the results by the name field.
func ByName(opts ...sql.OrderTermOption) OrderOption {
	return sql.OrderByField(FieldName, opts...).ToFunc()
}

// ByDescription orders the results by the description field.
func ByDescription(opts ...sql.OrderTermOption) OrderOption {
	return sql.OrderByField(FieldDescription, opts...).ToFunc()
}

// ByIsDisabled orders the results by the isDisabled field.
func ByIsDisabled(opts ...sql.OrderTermOption) OrderOption {
	return sql.OrderByField(FieldIsDisabled, opts...).ToFunc()
}

// ByParentField orders the results by parent field.
func ByParentField(field string, opts ...sql.OrderTermOption) OrderOption {
	return func(s *sql.Selector) {
		sqlgraph.OrderByNeighborTerms(s, newParentStep(), sql.OrderByField(field, opts...))
	}
}
func newParentStep() *sqlgraph.Step {
	return sqlgraph.NewStep(
		sqlgraph.From(Table, FieldID),
		sqlgraph.To(ParentInverseTable, FieldID),
		sqlgraph.Edge(sqlgraph.M2O, false, ParentTable, ParentColumn),
	)
}
