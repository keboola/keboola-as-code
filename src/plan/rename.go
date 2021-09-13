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
func Rename(projectState *state.State) (*RenamePlan, error) {
	builder := &renamePlanBuilder{State: projectState}
	actions, err := builder.build()
	if err != nil {
		return nil, err
	}
	return &RenamePlan{actions: actions}, nil
}

type renamePlanBuilder struct {
	*state.State
	actions []*RenameAction
}

func (b *renamePlanBuilder) build() ([]*RenameAction, error) {
	for _, object := range b.All() {
		action := &RenameAction{}

		// The parent object may have already been renamed, so update first old state
		if err := b.LocalManager().UpdatePaths(object, false); err != nil {
			return nil, err
		}
		action.OldPath = filepath.Join(b.ProjectDir(), object.RelativePath())

		// Rename
		if err := b.LocalManager().UpdatePaths(object, true); err != nil {
			return nil, err
		}
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
	return b.actions, nil
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

	for _, block := range config.Local.Blocks {
		b.renameBlock(block)
	}
}

func (b *renamePlanBuilder) renameBlock(block *model.Block) {
	// Update parent path
	b.LocalManager().UpdateBlockPath(block, false)

	// Store old path
	action := &RenameAction{}
	action.OldPath = filepath.Join(b.ProjectDir(), block.RelativePath())

	// Rename
	b.LocalManager().UpdateBlockPath(block, true)
	action.NewPath = filepath.Join(b.ProjectDir(), block.RelativePath())
	if action.OldPath != action.NewPath {
		b.actions = append(b.actions, action)
	}

	// Process codes
	for _, code := range block.Codes {
		b.renameCode(block, code)
	}
}

func (b *renamePlanBuilder) renameCode(block *model.Block, code *model.Code) {
	// Update parent path
	b.LocalManager().UpdateCodePath(block, code, false)

	// Store old path
	action := &RenameAction{}
	action.OldPath = filepath.Join(b.ProjectDir(), code.RelativePath())

	// Rename
	b.LocalManager().UpdateCodePath(block, code, true)
	action.NewPath = filepath.Join(b.ProjectDir(), code.RelativePath())
	if action.OldPath != action.NewPath {
		b.actions = append(b.actions, action)
	}

	// Rename code file
	b.renameCodeFile(code)
}

func (b *renamePlanBuilder) renameCodeFile(code *model.Code) {
	// Store old path
	action := &RenameAction{}
	action.OldPath = filepath.Join(b.ProjectDir(), b.Naming().CodeFilePath(code))

	// Rename
	code.CodeFileName = b.Naming().CodeFileName(code.ComponentId)
	action.NewPath = filepath.Join(b.ProjectDir(), b.Naming().CodeFilePath(code))
	if action.OldPath != action.NewPath {
		b.actions = append(b.actions, action)
	}
}
