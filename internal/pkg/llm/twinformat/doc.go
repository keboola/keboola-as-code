// Package twinformat implements the Twin Format specification for LLM data export.
//
// # JSON Output Format
//
// All JSON output from this package uses snake_case field names (e.g., "primary_key",
// "consumed_by", "component_id") as required by the Twin Format specification for
// LLM compatibility. This intentionally differs from Go's conventional camelCase.
//
// The tagliatelle linter is disabled at the package level in types.go for this reason.
//
// # Output Structure
//
// The export produces the following structure:
//   - tables/*.json - Table definitions with lineage dependencies
//   - transformations/*.json - Transformation configs with code blocks
//   - buckets/index.json - Bucket index with statistics
//   - lineage/graph.jsonl - Lineage edges in JSONL format
//   - manifest-extended.json - Project metadata and statistics
package twinformat
