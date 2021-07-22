package state

import (
	"fmt"
	"github.com/jpillora/longestcommon"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
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
		plan.OldPath = filepath.Join(s.ProjectDir(), object.RelativePath())

		// Rename
		object.UpdateManifest(s.manifest, true)
		plan.NewPath = filepath.Join(s.ProjectDir(), object.RelativePath())

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
		oldPathRel := utils.RelPath(s.ProjectDir(), plan.OldPath)
		newPathRel := utils.RelPath(s.ProjectDir(), plan.NewPath)
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
		plans = append(plans, s.renameBlock(config.ConfigManifest, index, block)...)
	}

	return plans
}

func (s *State) renameBlock(config *manifest.ConfigManifest, index int, block *model.Block) (plans []*model.RenamePlan) {
	// Update parent path
	block.ParentPath = s.Naming().BlocksDir(config.RelativePath())

	// Store old path
	plan := &model.RenamePlan{}
	plan.OldPath = filepath.Join(s.ProjectDir(), block.RelativePath())

	// Rename
	block.Path = s.Naming().BlockPath(index, block.Name)
	plan.NewPath = filepath.Join(s.ProjectDir(), block.RelativePath())
	if plan.OldPath != plan.NewPath {
		plans = append(plans, plan)
	}

	// Process codes
	for index, code := range block.Codes {
		plans = append(plans, s.renameCode(config.ComponentId, block, index, code)...)
	}

	return plans
}

func (s *State) renameCode(componentId string, block *model.Block, index int, code *model.Code) (plans []*model.RenamePlan) {
	// Update parent path
	code.ParentPath = block.RelativePath()

	// Store old path
	plan := &model.RenamePlan{}
	plan.OldPath = filepath.Join(s.ProjectDir(), code.RelativePath())

	// Rename
	code.Path = s.Naming().CodePath(index, code.Name)
	plan.NewPath = filepath.Join(s.ProjectDir(), code.RelativePath())
	if plan.OldPath != plan.NewPath {
		plans = append(plans, plan)
	}

	// Rename code file
	plans = append(plans, s.renameCodeFile(componentId, code)...)

	return plans
}

func (s *State) renameCodeFile(componentId string, code *model.Code) (plans []*model.RenamePlan) {
	// Store old path
	plan := &model.RenamePlan{}
	plan.OldPath = filepath.Join(s.ProjectDir(), code.CodeFilePath())

	// Rename
	code.CodeFileName = s.Naming().CodeFileName(componentId)
	plan.NewPath = filepath.Join(s.ProjectDir(), code.CodeFilePath())
	if plan.OldPath != plan.NewPath {
		plans = append(plans, plan)
	}

	return plans
}
