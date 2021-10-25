package plan

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/strhelper"
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
		action.OldPath = object.Path()

		// Rename
		if err := b.LocalManager().UpdatePaths(object, true); err != nil {
			return nil, err
		}
		action.NewPath = object.Path()

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
		action.Description = strhelper.FormatPathChange(action.OldPath, action.NewPath, false)
	}
}

func (b *renamePlanBuilder) renameBlocks(config *model.ConfigState) {
	if config.Local == nil {
		return
	}

	configDir := config.Path()
	for _, block := range config.Local.Blocks {
		b.renameBlock(block, configDir)
	}
}

func (b *renamePlanBuilder) renameBlock(block *model.Block, configDir string) {
	// Update parent path
	b.LocalManager().UpdateBlockPath(block, configDir, false)

	// Store old path
	action := &RenameAction{}
	action.OldPath = block.Path()

	// Rename
	b.LocalManager().UpdateBlockPath(block, configDir, true)
	action.NewPath = block.Path()
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
	action.OldPath = code.Path()

	// Rename
	b.LocalManager().UpdateCodePath(block, code, true)
	action.NewPath = code.Path()
	if action.OldPath != action.NewPath {
		b.actions = append(b.actions, action)
	}

	// Rename code file
	b.renameCodeFile(code)
}

func (b *renamePlanBuilder) renameCodeFile(code *model.Code) {
	// Store old path
	action := &RenameAction{}
	action.OldPath = b.Naming().CodeFilePath(code)

	// Rename
	code.CodeFileName = b.Naming().CodeFileName(code.ComponentId)
	action.NewPath = b.Naming().CodeFilePath(code)
	if action.OldPath != action.NewPath {
		b.actions = append(b.actions, action)
	}
}
