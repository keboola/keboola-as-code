package state

import (
	"fmt"
	"github.com/jpillora/longestcommon"
	"keboola-as-code/src/model"
	"keboola-as-code/src/transformation"
	"keboola-as-code/src/utils"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// RenamePlan creates a plan for renaming objects that do not match the naming
func (s *State) RenamePlan() (plans []*model.RenamePlan) {
	for _, object := range s.All() {
		plan := &model.RenamePlan{}

		// The parent object may have already been renamed, so update first old state
		object.UpdateManifest(s.manifest, false)
		plan.OldPath = filepath.Join(s.manifest.ProjectDir, object.RelativePath())

		// Rename
		object.UpdateManifest(s.manifest, true)
		plan.NewPath = filepath.Join(s.manifest.ProjectDir, object.RelativePath())

		// Should be renamed?
		if plan.OldPath != plan.NewPath {
			// Add to plan
			s.manifest.PersistRecord(object.Manifest())
			plans = append(plans, plan)
		}

		// Rename transformation blocks
		if v, ok := object.(*ConfigState); ok {
			plans = append(plans, s.renameBlocks(v)...)
		}
	}

	// Set description
	for _, plan := range plans {
		// Get common prefix of the old and new path
		oldPathRel := utils.RelPath(s.manifest.ProjectDir, plan.OldPath)
		newPathRel := utils.RelPath(s.manifest.ProjectDir, plan.NewPath)
		prefix := longestcommon.Prefix([]string{oldPathRel, newPathRel})

		// Remove from the prefix everything after the last separator
		prefix = regexp.
			MustCompile(fmt.Sprintf(`(^|%c)[^%c]*$`, os.PathSeparator, os.PathSeparator)).
			ReplaceAllString(prefix, "$1")

		// Generate description for logs
		if prefix != "" {
			plan.Description = fmt.Sprintf(
				`%s{%s -> %s}`,
				prefix,
				strings.TrimPrefix(oldPathRel, prefix),
				strings.TrimPrefix(newPathRel, prefix),
			)
		} else {
			plan.Description = fmt.Sprintf(`%s -> %s`, oldPathRel, newPathRel)
		}
	}

	return plans
}

func (s *State) renameBlocks(config *ConfigState) (plans []*model.RenamePlan) {
	if config.Local == nil {
		return
	}

	for index, block := range config.Local.Blocks {
		plans = append(plans, transformation.RenameBlock(s.manifest.ProjectDir, config.ConfigManifest, index, block)...)
	}

	return plans
}
