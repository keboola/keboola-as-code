package twinformat

import (
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
)

// Column types and descriptions come from the native table definition only.
// A typed table carries type, base type, nullable, length and description per column.
// An untyped table carries descriptions only: it has no native column type, so no type is written for it.

// columnsByName indexes the columns of the table definition by name; it is empty when the definition is nil.
func columnsByName(definition *keboola.TableDefinition) map[string]keboola.Column {
	columns := make(map[string]keboola.Column)
	if definition == nil {
		return columns
	}
	for _, col := range definition.Columns {
		columns[col.Name] = col
	}
	return columns
}

// buildColumnDetailEntry builds a detailed column entry map for table metadata.
func buildColumnDetailEntry(colName string, columns map[string]keboola.Column) map[string]any {
	entry := map[string]any{"name": colName}
	col, ok := columns[colName]
	if !ok || col.Definition == nil {
		return entry
	}

	def := col.Definition
	if def.Description != "" {
		entry["description"] = def.Description
	}
	if def.Type == "" {
		return entry
	}

	entry["type"] = def.Type
	entry["nullable"] = def.Nullable
	if def.Length != "" {
		entry["length"] = def.Length
	}
	if col.BaseType != nil {
		entry["base_type"] = string(*col.BaseType)
	}
	return entry
}

// buildColumnEntry builds a column entry map for the data dictionary; its type is the column base type.
func buildColumnEntry(colName string, columns map[string]keboola.Column) map[string]any {
	entry := map[string]any{"name": colName}
	col, ok := columns[colName]
	if !ok || col.Definition == nil {
		return entry
	}

	if col.Definition.Description != "" {
		entry["description"] = col.Definition.Description
	}
	if col.Definition.Type != "" && col.BaseType != nil {
		entry["type"] = string(*col.BaseType)
	}
	return entry
}
