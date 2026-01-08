package twinformat

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// Edge types for the lineage graph.
const (
	EdgeTypeConsumedBy = "consumed_by"
	EdgeTypeProduces   = "produces"
)

// LineageBuilderDependencies defines dependencies for the LineageBuilder.
type LineageBuilderDependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

// LineageBuilder builds the lineage graph from transformations.
type LineageBuilder struct {
	logger    log.Logger
	telemetry telemetry.Telemetry
}

// NewLineageBuilder creates a new LineageBuilder instance.
func NewLineageBuilder(d LineageBuilderDependencies) *LineageBuilder {
	return &LineageBuilder{
		logger:    d.Logger(),
		telemetry: d.Telemetry(),
	}
}

// LineageGraph represents the complete lineage graph.
type LineageGraph struct {
	Edges      []*LineageEdge
	Meta       *LineageMetaData
	NodeCount  int
	TableNodes map[string]bool // Set of table node IDs
	TransNodes map[string]bool // Set of transformation node IDs
}

// BuildLineageGraph builds the lineage graph from scanned transformations.
func (b *LineageBuilder) BuildLineageGraph(ctx context.Context, transformations []*ScannedTransformation) (graph *LineageGraph, err error) {
	ctx, span := b.telemetry.Tracer().Start(ctx, "keboola.go.twinformat.lineage.BuildLineageGraph")
	defer span.End(&err)

	graph = &LineageGraph{
		Edges:      make([]*LineageEdge, 0),
		TableNodes: make(map[string]bool),
		TransNodes: make(map[string]bool),
	}

	for _, t := range transformations {
		transformUID := b.buildTransformationUID(t)
		graph.TransNodes[transformUID] = true

		// Build input edges: table -> transformation (consumed_by)
		for _, input := range t.InputTables {
			tableUID := b.buildTableUID(input.Source)
			graph.TableNodes[tableUID] = true

			edge := &LineageEdge{
				Source: tableUID,
				Target: transformUID,
				Type:   EdgeTypeConsumedBy,
			}
			graph.Edges = append(graph.Edges, edge)
		}

		// Build output edges: transformation -> table (produces)
		for _, output := range t.OutputTables {
			tableUID := b.buildTableUID(output.Destination)
			graph.TableNodes[tableUID] = true

			edge := &LineageEdge{
				Source: transformUID,
				Target: tableUID,
				Type:   EdgeTypeProduces,
			}
			graph.Edges = append(graph.Edges, edge)
		}
	}

	// Calculate node count
	graph.NodeCount = len(graph.TableNodes) + len(graph.TransNodes)

	// Build metadata
	graph.Meta = &LineageMetaData{
		TotalEdges: len(graph.Edges),
		TotalNodes: graph.NodeCount,
		Updated:    time.Now().UTC().Format(time.RFC3339),
	}

	b.logger.Infof(ctx, "Built lineage graph with %d edges and %d nodes", len(graph.Edges), graph.NodeCount)

	return graph, nil
}

// BuildLineageGraphFromAPI builds the lineage graph from API-fetched transformation configs.
func (b *LineageBuilder) BuildLineageGraphFromAPI(ctx context.Context, configs []*TransformationConfig) (graph *LineageGraph, err error) {
	ctx, span := b.telemetry.Tracer().Start(ctx, "keboola.go.twinformat.lineage.BuildLineageGraphFromAPI")
	defer span.End(&err)

	graph = &LineageGraph{
		Edges:      make([]*LineageEdge, 0),
		TableNodes: make(map[string]bool),
		TransNodes: make(map[string]bool),
	}

	for _, cfg := range configs {
		transformUID := b.buildTransformationUIDFromConfig(cfg)
		graph.TransNodes[transformUID] = true

		// Build input edges: table -> transformation (consumed_by)
		for _, input := range cfg.InputTables {
			tableUID := b.buildTableUID(input.Source)
			graph.TableNodes[tableUID] = true

			edge := &LineageEdge{
				Source: tableUID,
				Target: transformUID,
				Type:   EdgeTypeConsumedBy,
			}
			graph.Edges = append(graph.Edges, edge)
		}

		// Build output edges: transformation -> table (produces)
		for _, output := range cfg.OutputTables {
			tableUID := b.buildTableUID(output.Destination)
			graph.TableNodes[tableUID] = true

			edge := &LineageEdge{
				Source: transformUID,
				Target: tableUID,
				Type:   EdgeTypeProduces,
			}
			graph.Edges = append(graph.Edges, edge)
		}
	}

	// Calculate node count
	graph.NodeCount = len(graph.TableNodes) + len(graph.TransNodes)

	// Build metadata
	graph.Meta = &LineageMetaData{
		TotalEdges: len(graph.Edges),
		TotalNodes: graph.NodeCount,
		Updated:    time.Now().UTC().Format(time.RFC3339),
	}

	b.logger.Infof(ctx, "Built lineage graph from API with %d edges and %d nodes", len(graph.Edges), graph.NodeCount)

	return graph, nil
}

// buildTransformationUID builds a UID for a transformation.
// Format: transform:{name}.
func (b *LineageBuilder) buildTransformationUID(t *ScannedTransformation) string {
	name := t.Name
	if name == "" {
		name = t.ConfigID
	}
	return fmt.Sprintf("transform:%s", sanitizeUID(name))
}

// buildTransformationUIDFromConfig builds a UID for a transformation from API config.
func (b *LineageBuilder) buildTransformationUIDFromConfig(cfg *TransformationConfig) string {
	name := cfg.Name
	if name == "" {
		name = cfg.ID
	}
	return fmt.Sprintf("transform:%s", sanitizeUID(name))
}

// buildTableUID builds a UID for a table from a table reference.
// Input format: "in.c-bucket.table" or "out.c-bucket.table".
// Output format: "table:bucket/table".
func (b *LineageBuilder) buildTableUID(tableRef string) string {
	// Parse table reference
	parts := strings.Split(tableRef, ".")
	if len(parts) < 3 {
		// Fallback: use the whole reference
		return fmt.Sprintf("table:%s", sanitizeUID(tableRef))
	}

	// Extract bucket and table name
	// Format: stage.c-bucket.table -> bucket/table
	bucket := strings.TrimPrefix(parts[1], "c-")
	table := strings.Join(parts[2:], ".")

	return fmt.Sprintf("table:%s/%s", sanitizeUID(bucket), sanitizeUID(table))
}

// sanitizeUID sanitizes a string for use in a UID.
// Transformation rules:
//   - Spaces are replaced with underscores
//   - Hyphens are replaced with underscores
//   - All characters are lowercased
//
// Note: This may cause collisions (e.g., "my-table" and "my_table" both become "my_table").
// In practice, collisions are rare since Keboola bucket/table names typically follow
// consistent naming conventions within a project.
func sanitizeUID(s string) string {
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, "-", "_")
	return strings.ToLower(s)
}

// GetTableDependencies returns the dependencies for a specific table.
func (b *LineageBuilder) GetTableDependencies(graph *LineageGraph, tableUID string) *TableDependencies {
	deps := &TableDependencies{
		ConsumedBy: make([]string, 0),
		ProducedBy: make([]string, 0),
	}

	for _, edge := range graph.Edges {
		// Table is consumed by transformation (table -> transform)
		if edge.Source == tableUID && edge.Type == EdgeTypeConsumedBy {
			deps.ConsumedBy = append(deps.ConsumedBy, edge.Target)
		}
		// Table is produced by transformation (transform -> table)
		if edge.Target == tableUID && edge.Type == EdgeTypeProduces {
			deps.ProducedBy = append(deps.ProducedBy, edge.Source)
		}
	}

	return deps
}

// GetTransformationDependencies returns the dependencies for a specific transformation.
func (b *LineageBuilder) GetTransformationDependencies(graph *LineageGraph, transformUID string) *TransformationDependencies {
	deps := &TransformationDependencies{
		Consumes: make([]string, 0),
		Produces: make([]string, 0),
	}

	for _, edge := range graph.Edges {
		// Transformation consumes table (table -> transform)
		if edge.Target == transformUID && edge.Type == EdgeTypeConsumedBy {
			deps.Consumes = append(deps.Consumes, edge.Source)
		}
		// Transformation produces table (transform -> table)
		if edge.Source == transformUID && edge.Type == EdgeTypeProduces {
			deps.Produces = append(deps.Produces, edge.Target)
		}
	}

	return deps
}

// BuildTableUIDFromParts builds a table UID from bucket and table name.
func BuildTableUIDFromParts(bucket, table string) string {
	return fmt.Sprintf("table:%s/%s", sanitizeUID(bucket), sanitizeUID(table))
}

// BuildTransformationUIDFromName builds a transformation UID from a name.
func BuildTransformationUIDFromName(name string) string {
	return fmt.Sprintf("transform:%s", sanitizeUID(name))
}

// ParseTableUID parses a table UID into bucket and table name.
// Input format: "table:bucket/table".
// Returns: bucket, table.
func ParseTableUID(uid string) (bucket, table string) {
	// Remove "table:" prefix
	uid = strings.TrimPrefix(uid, "table:")

	// Split by "/"
	parts := strings.SplitN(uid, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}

	return uid, ""
}

// ParseTransformationUID parses a transformation UID into name.
// Input format: "transform:name".
// Returns: name.
func ParseTransformationUID(uid string) string {
	return strings.TrimPrefix(uid, "transform:")
}
