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

// uidTracker tracks original inputs to sanitized UIDs for collision detection.
type uidTracker struct {
	tableUIDs     map[string]string // sanitized UID -> original input
	transformUIDs map[string]string // sanitized UID -> original input
}

// newUIDTracker creates a new UID tracker.
func newUIDTracker() *uidTracker {
	return &uidTracker{
		tableUIDs:     make(map[string]string),
		transformUIDs: make(map[string]string),
	}
}

// trackTableUID tracks a table UID and returns true if a collision is detected.
// If a collision is detected, it returns the previous original input.
func (t *uidTracker) trackTableUID(uid, original string) (collision bool, previousOriginal string) {
	if prev, exists := t.tableUIDs[uid]; exists && prev != original {
		return true, prev
	}
	t.tableUIDs[uid] = original
	return false, ""
}

// trackTransformUID tracks a transformation UID and returns true if a collision is detected.
// If a collision is detected, it returns the previous original input.
func (t *uidTracker) trackTransformUID(uid, original string) (collision bool, previousOriginal string) {
	if prev, exists := t.transformUIDs[uid]; exists && prev != original {
		return true, prev
	}
	t.transformUIDs[uid] = original
	return false, ""
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

	// Track UIDs for collision detection
	tracker := newUIDTracker()

	for _, cfg := range configs {
		transformUID := b.buildTransformationUIDFromConfig(cfg)
		originalName := cfg.Name
		if originalName == "" {
			originalName = cfg.ID
		}
		if collision, prev := tracker.trackTransformUID(transformUID, originalName); collision {
			b.logger.Warnf(ctx, "UID collision detected: transformations %q and %q both map to UID %q", prev, originalName, transformUID)
		}
		graph.TransNodes[transformUID] = true

		// Build input edges: table -> transformation (consumed_by)
		for _, input := range cfg.InputTables {
			tableUID := b.buildTableUID(ctx, input.Source)
			if collision, prev := tracker.trackTableUID(tableUID, input.Source); collision {
				b.logger.Warnf(ctx, "UID collision detected: tables %q and %q both map to UID %q", prev, input.Source, tableUID)
			}
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
			tableUID := b.buildTableUID(ctx, output.Destination)
			if collision, prev := tracker.trackTableUID(tableUID, output.Destination); collision {
				b.logger.Warnf(ctx, "UID collision detected: tables %q and %q both map to UID %q", prev, output.Destination, tableUID)
			}
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
func (b *LineageBuilder) buildTableUID(ctx context.Context, tableRef string) string {
	// Parse table reference
	parts := strings.Split(tableRef, ".")
	if len(parts) < 3 {
		// Fallback: use the whole reference
		b.logger.Debugf(ctx, "Malformed table reference %q (expected format: stage.bucket.table), using fallback UID", tableRef)
		return fmt.Sprintf("table:%s", sanitizeUID(tableRef))
	}

	// Extract bucket and table name
	// Format: stage.c-bucket.table -> bucket/table
	bucket := strings.TrimPrefix(parts[1], "c-")
	table := strings.Join(parts[2:], ".")

	return fmt.Sprintf("table:%s/%s", sanitizeUID(bucket), sanitizeUID(table))
}

// sanitizeUID sanitizes a string for use in a UID.
//
// Transformation rules:
//   - Spaces are replaced with underscores
//   - Hyphens are replaced with underscores
//   - All characters are lowercased
//
// # Collision Warning
//
// This sanitization may cause UID collisions. Examples of inputs that produce
// the same output:
//   - "my-table" and "my_table" both become "my_table"
//   - "My Table" and "my_table" both become "my_table"
//   - "data-2024" and "data_2024" both become "data_2024"
//
// In the context of data lineage tracking, collisions could cause:
//   - Incorrect dependency relationships between tables/transformations
//   - Missing or merged lineage edges in the graph
//
// In practice, collisions are rare because Keboola bucket/table names typically
// follow consistent naming conventions within a project (either hyphens or
// underscores, not both). If collision detection is needed, callers should
// track sanitized UIDs and check for duplicates.
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
