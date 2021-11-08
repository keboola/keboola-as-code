package plan

import (
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
	pathsUpdater := b.LocalManager().NewPathsGenerator(true)
	for _, object := range b.All() {
		if err := pathsUpdater.Update(object); err != nil {
			return nil, err
		}
	}

	// Convert renamed items to actions
	for _, item := range pathsUpdater.Renamed() {
		action := &RenameAction{
			OldPath:     item.OldPath,
			RenameFrom:  item.RenameFrom,
			NewPath:     item.NewPath,
			Description: strhelper.FormatPathChange(item.RenameFrom, item.NewPath, false),
		}
		if item.ObjectState != nil {
			action.Record = item.ObjectState.Manifest()
		}
		b.actions = append(b.actions, action)
	}

	return b.actions, nil
}
