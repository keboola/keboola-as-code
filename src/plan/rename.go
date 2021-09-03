package plan

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/jpillora/longestcommon"

	"keboola-as-code/src/model"
	"keboola-as-code/src/state"
	"keboola-as-code/src/utils"
)

// Rename creates a plan for renaming objects that do not match the naming.
func Rename(projectState *state.State) *RenamePlan {
	builder := &renamePlanBuilder{State: projectState}
	return &RenamePlan{actions: builder.build()}
}

type renamePlanBuilder struct {
	*state.State
	actions []*RenameAction
}

func (b *renamePlanBuilder) build() []*RenameAction {
	for _, object := range b.All() {
		action := &RenameAction{}

		// The parent object may have already been renamed, so update first old state
		b.LocalManager().UpdatePaths(object, false)
		action.OldPath = filepath.Join(b.ProjectDir(), object.RelativePath())

		// Rename
		b.LocalManager().UpdatePaths(object, true)
		action.NewPath = filepath.Join(b.ProjectDir(), object.RelativePath())

		// Should be renamed?
		if action.OldPath != action.NewPath {
			// Add to plan
			action.Record = object.Manifest()
			b.actions = append(b.actions, action)
		}

		// Rename transformation blocks
		if v, ok := object.(*model.ConfigState); ok {
			b.renameBlocks(v)
		}
	}

	b.setDescriptions()
	return b.actions
}

func (b *renamePlanBuilder) setDescriptions() {
	// Set description
	for _, action := range b.actions {
		// Get common prefix of the old and new path
		oldPathRel := utils.RelPath(b.ProjectDir(), action.OldPath)
		newPathRel := utils.RelPath(b.ProjectDir(), action.NewPath)
		prefix := longestcommon.Prefix([]string{oldPathRel, newPathRel})

		// Remove from the prefix everything after the last separator
		prefix = regexp.
			MustCompile(fmt.Sprintf(`(^|%c)[^%c]*$`, os.PathSeparator, os.PathSeparator)).
			ReplaceAllString(prefix, "$1")

		// Generate description for logs
		if prefix != "" {
			action.Description = fmt.Sprintf(
				`%s{%s -> %s}`,
				prefix,
				strings.TrimPrefix(oldPathRel, prefix),
				strings.TrimPrefix(newPathRel, prefix),
			)
		} else {
			action.Description = fmt.Sprintf(`%s -> %s`, oldPathRel, newPathRel)
		}
	}
}

func (b *renamePlanBuilder) renameBlocks(config *model.ConfigState) {
	if config.Local == nil {
		return
	}

	for index, block := range config.Local.Blocks {
		b.renameBlock(config.ConfigManifest, index, block)
	}
}

func (b *renamePlanBuilder) renameBlock(config *model.ConfigManifest, index int, block *model.Block) {
	// Update parent path
	block.ParentPath = b.Naming().BlocksDir(config.RelativePath())

	// Store old path
	action := &RenameAction{}
	action.OldPath = filepath.Join(b.ProjectDir(), block.RelativePath())

	// Rename
	block.Path = b.Naming().BlockPath(index, block.Name)
	action.NewPath = filepath.Join(b.ProjectDir(), block.RelativePath())
	if action.OldPath != action.NewPath {
		b.actions = append(b.actions, action)
	}

	// Process codes
	for codeIndex, code := range block.Codes {
		b.renameCode(config.ComponentId, block, codeIndex, code)
	}
}

func (b *renamePlanBuilder) renameCode(componentId string, block *model.Block, index int, code *model.Code) {
	// Update parent path
	code.ParentPath = block.RelativePath()

	// Store old path
	action := &RenameAction{}
	action.OldPath = filepath.Join(b.ProjectDir(), code.RelativePath())

	// Rename
	code.Path = b.Naming().CodePath(index, code.Name)
	action.NewPath = filepath.Join(b.ProjectDir(), code.RelativePath())
	if action.OldPath != action.NewPath {
		b.actions = append(b.actions, action)
	}

	// Rename code file
	b.renameCodeFile(componentId, code)
}

func (b *renamePlanBuilder) renameCodeFile(componentId string, code *model.Code) {
	// Store old path
	action := &RenameAction{}
	action.OldPath = filepath.Join(b.ProjectDir(), b.Naming().CodeFilePath(code))

	// Rename
	code.CodeFileName = b.Naming().CodeFileName(componentId)
	action.NewPath = filepath.Join(b.ProjectDir(), b.Naming().CodeFilePath(code))
	if action.OldPath != action.NewPath {
		b.actions = append(b.actions, action)
	}
}
