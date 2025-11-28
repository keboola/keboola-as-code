package ignore

import (
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
)

// IgnoreConfigsOrRows applies ignore patterns and validates that no orchestrators reference ignored configs.
// Returns an error listing orchestrators that need to be explicitly added to .kbcignore.
func (f *File) IgnoreConfigsOrRows() error {
	if err := f.applyIgnoredPatterns(); err != nil {
		return err
	}
	return f.validateOrchestratorDependencies()
}

// applyIgnoredPatterns parses the content for ignore patterns and applies them to configurations or rows.
func (f *File) applyIgnoredPatterns() error {
	for _, pattern := range f.parseIgnoredPatterns() {
		if err := f.applyIgnorePattern(pattern); err != nil {
			continue
		}
	}
	return nil
}

func (f *File) parseIgnoredPatterns() []string {
	var ignorePatterns []string
	lines := strings.SplitSeq(f.rawStringPattern, "\n")
	for line := range lines {
		trimmedLine := strings.TrimSpace(line)
		// Skip empty lines and comments
		if trimmedLine != "" && !strings.HasPrefix(trimmedLine, "#") {
			ignorePatterns = append(ignorePatterns, trimmedLine)
		}
	}

	return ignorePatterns
}

// applyIgnorePattern applies a single ignore pattern, marking the appropriate config or row as ignored.
func (f *File) applyIgnorePattern(ignoreConfig string) error {
	parts := strings.Split(ignoreConfig, "/")

	switch len(parts) {
	case 2:
		// Ignore config by ID and name.
		configID, componentID := parts[1], parts[0]
		f.state.IgnoreConfig(configID, componentID)
	case 3:
		// Ignore specific config row.
		configID, rowID := parts[1], parts[2]
		f.state.IgnoreConfigRow(configID, rowID)
	default:
		return errors.Errorf("invalid ignore ignoreConfig format: %s", ignoreConfig)
	}

	return nil
}

// validateOrchestratorDependencies checks if any non-ignored configs with reverse dependencies
// (orchestrators, schedulers, input mappings, etc.) reference ignored configs.
// Returns an error instructing the user to explicitly add those dependent configs to .kbcignore.
func (f *File) validateOrchestratorDependencies() error {
	errs := errors.NewMultiError()

	// Build map of ignored config IDs for quick lookup
	ignoredConfigs := make(map[string]bool)
	for _, cfg := range f.state.IgnoredConfigs() {
		ignoredConfigs[cfg.ID.String()] = true
	}

	// Relation types where the "owner" config (the one with the relation) depends on the "target" config
	// If target is ignored, owner should also be ignored
	reverseDependencyRelations := []model.RelationType{
		model.UsedInOrchestratorRelType,       // Orchestrator tasks reference other configs
		model.UsedInConfigInputMappingRelType, // Config input mappings reference source configs
		model.UsedInRowInputMappingRelType,    // Row input mappings reference source configs
		model.SchedulerForRelType,             // Schedulers reference target configs to run
	}

	// Check each config for reverse dependency relations
	for _, targetCfg := range f.state.Configs() {
		// Skip if this config is not ignored (only care about ignored configs being referenced)
		if !ignoredConfigs[targetCfg.ID.String()] {
			continue
		}

		// Check each reverse dependency relation type
		for _, relType := range reverseDependencyRelations {
			rels := targetCfg.Relations.GetByType(relType)
			for _, r := range rels {
				// Extract the dependent config ID from the relation
				dependentConfigID := f.extractDependentConfigID(r)
				if dependentConfigID == "" {
					continue
				}

				// Find the dependent config in the registry
				dependentCfg := f.findConfigByID(targetCfg.BranchID, dependentConfigID)
				if dependentCfg == nil {
					continue
				}

				// If dependent config is NOT ignored but references an ignored config, report error
				if !dependentCfg.Ignore {
					dependentPath := dependentCfg.Path()
					if dependentPath == "" {
						dependentPath = dependentCfg.ConfigKey.Desc()
					}
					targetPath := targetCfg.Path()
					if targetPath == "" {
						targetPath = targetCfg.ConfigKey.Desc()
					}

					relTypeDesc := f.getRelationTypeDescription(relType)
					errs.Append(errors.Errorf(
						"%s %q references ignored config %q, please add it to .kbcignore:\n  %s/%s",
						relTypeDesc,
						dependentPath,
						targetPath,
						dependentCfg.ComponentID,
						dependentCfg.ID,
					))
				}
			}
		}
	}

	if errs.Len() > 0 {
		return errors.PrefixError(errs, "configurations with dependencies reference ignored configurations")
	}
	return nil
}

// extractDependentConfigID extracts the config ID of the dependent config from a relation.
// Returns empty string if the relation doesn't contain a config ID reference.
func (f *File) extractDependentConfigID(rel model.Relation) string {
	switch r := rel.(type) {
	case *model.UsedInOrchestratorRelation:
		return r.ConfigID.String()
	case *model.SchedulerForRelation:
		return r.ConfigID.String()
	case *model.UsedInConfigInputMappingRelation:
		return r.UsedIn.ID.String()
	case *model.UsedInRowInputMappingRelation:
		return r.UsedIn.ConfigID.String()
	default:
		return ""
	}
}

// findConfigByID finds a config by ID within the same branch.
func (f *File) findConfigByID(branchID keboola.BranchID, configID string) *model.ConfigState {
	for _, cfg := range f.state.Configs() {
		if cfg.BranchID == branchID && cfg.ID.String() == configID {
			return cfg
		}
	}
	return nil
}

// getRelationTypeDescription returns a human-readable description of the relation type.
func (f *File) getRelationTypeDescription(relType model.RelationType) string {
	switch relType {
	case model.UsedInOrchestratorRelType:
		return "orchestrator"
	case model.SchedulerForRelType:
		return "scheduler"
	case model.UsedInConfigInputMappingRelType:
		return "config with input mapping"
	case model.UsedInRowInputMappingRelType:
		return "config row with input mapping"
	default:
		return "configuration"
	}
}
