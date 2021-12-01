package rename

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/strhelper"
)

// NewPlan creates a plan for renaming objects that do not match the naming.
func NewPlan(projectState *state.State) (*Plan, error) {
	builder := &renamePlanBuilder{State: projectState}
	actions, err := builder.build()
	if err != nil {
		return nil, err
	}
	return &Plan{actions: actions}, nil
}

type renamePlanBuilder struct {
	*state.State
	actions []model.RenameAction
}

func (b *renamePlanBuilder) build() ([]model.RenameAction, error) {
	pathsUpdater := b.LocalManager().NewPathsGenerator(true)
	for _, object := range b.All() {
		pathsUpdater.Add(object)
	}
	if err := pathsUpdater.Invoke(); err != nil {
		return nil, err
	}

	// Convert renamed items to actions
	for _, item := range pathsUpdater.Renamed() {
		action := model.RenameAction{
			OldPath:     item.OldPath,
			RenameFrom:  item.RenameFrom,
			NewPath:     item.NewPath,
			Description: strhelper.FormatPathChange(item.RenameFrom, item.NewPath, false),
		}
		if item.ObjectState != nil {
			action.Manifest = item.ObjectState.Manifest()
		}
		b.actions = append(b.actions, action)
	}

	return b.actions, nil
}
