package twinformat

import (
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
)

// Source constants for basic source types.
const (
	SourceUnknown = "unknown"
)

// ComponentInfo holds metadata about a Keboola component.
type ComponentInfo struct {
	ID   string // Component ID (e.g., "kds-team.app-custom-python")
	Type string // Component type ("extractor", "transformation", "application", "writer")
	Name string // Human-readable name (e.g., "Custom Python")
}

// ComponentRegistry maps component IDs to their metadata.
type ComponentRegistry struct {
	components map[string]ComponentInfo
}

// NewComponentRegistry creates a new ComponentRegistry.
func NewComponentRegistry() *ComponentRegistry {
	return &ComponentRegistry{
		components: make(map[string]ComponentInfo),
	}
}

// Register adds a component to the registry.
func (r *ComponentRegistry) Register(comp *keboola.ComponentWithConfigs) {
	if comp == nil {
		return
	}
	r.components[comp.ID.String()] = ComponentInfo{
		ID:   comp.ID.String(),
		Type: comp.Type,
		Name: comp.Name,
	}
}

// GetType returns the type for a component ID.
// Returns empty string if component not found.
func (r *ComponentRegistry) GetType(componentID string) string {
	if info, ok := r.components[componentID]; ok {
		return info.Type
	}
	return ""
}

// GetName returns the human-readable name for a component ID.
// Returns empty string if component not found.
func (r *ComponentRegistry) GetName(componentID string) string {
	if info, ok := r.components[componentID]; ok {
		return info.Name
	}
	return ""
}

// Get returns the ComponentInfo for a component ID.
// Returns empty ComponentInfo if not found.
func (r *ComponentRegistry) Get(componentID string) (ComponentInfo, bool) {
	info, ok := r.components[componentID]
	return info, ok
}

// TableSource represents the source of a table.
type TableSource struct {
	ComponentID   string // Component ID that created this table (e.g., "kds-team.app-custom-python")
	ComponentType string // Component type ("extractor", "transformation", "application")
	ConfigID      string // Configuration ID
	ConfigName    string // Configuration name
}

// TableSourceRegistry maps table IDs to their source components.
type TableSourceRegistry struct {
	tableToSource map[string]TableSource
}

// NewTableSourceRegistry creates a new TableSourceRegistry.
func NewTableSourceRegistry() *TableSourceRegistry {
	return &TableSourceRegistry{
		tableToSource: make(map[string]TableSource),
	}
}

// Register adds a table source mapping.
func (r *TableSourceRegistry) Register(tableID string, source TableSource) {
	if tableID == "" {
		return
	}
	r.tableToSource[tableID] = source
}

// GetSource returns the source ComponentID for a table.
// Returns SourceUnknown if table not found.
func (r *TableSourceRegistry) GetSource(tableID string) string {
	if source, ok := r.tableToSource[tableID]; ok {
		return source.ComponentID
	}
	return SourceUnknown
}

// GetSourceInfo returns the full TableSource for a table.
// Returns empty TableSource and false if not found.
func (r *TableSourceRegistry) GetSourceInfo(tableID string) (TableSource, bool) {
	source, ok := r.tableToSource[tableID]
	return source, ok
}

// GetDominantSourceForBucket returns the most common source ComponentID
// among tables in the given bucket.
// Returns SourceUnknown if no tables found.
func (r *TableSourceRegistry) GetDominantSourceForBucket(bucketID string, tableIDs []string) string {
	if len(tableIDs) == 0 {
		return SourceUnknown
	}

	// Count sources
	sourceCounts := make(map[string]int)
	for _, tableID := range tableIDs {
		source := r.GetSource(tableID)
		if source != SourceUnknown {
			sourceCounts[source]++
		}
	}

	if len(sourceCounts) == 0 {
		return SourceUnknown
	}

	// Find dominant source (use alphabetical order as tiebreaker for deterministic results)
	var dominantSource string
	var maxCount int
	for source, count := range sourceCounts {
		if count > maxCount || (count == maxCount && source < dominantSource) {
			maxCount = count
			dominantSource = source
		}
	}

	return dominantSource
}
