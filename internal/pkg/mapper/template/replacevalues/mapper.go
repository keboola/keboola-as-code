package replacevalues

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type replaceKeysMapper struct {
	state        *state.State
	logger       log.Logger
	replacements *Values
}

type changes interface {
	Loaded() []model.ObjectState
	Replace(callback model.ChangesReplaceFunc)
}

func NewMapper(state *state.State, replacements *Values) *replaceKeysMapper {
	return &replaceKeysMapper{state: state, logger: state.Logger(), replacements: replacements}
}

func (m *replaceKeysMapper) AfterLocalOperation(changes *model.LocalChanges) error {
	return m.afterOperation(changes)
}

func (m *replaceKeysMapper) AfterRemoteOperation(changes *model.RemoteChanges) error {
	return m.afterOperation(changes)
}

func (m *replaceKeysMapper) afterOperation(changes changes) error {
	// Replace keys in the loaded remote objects
	replaced := make(map[string]model.ObjectState)
	errors := utils.NewMultiError()
	for _, original := range changes.Loaded() {
		// Replace values
		modifiedRaw, err := m.replacements.Replace(original)
		if err != nil {
			errors.Append(err)
			continue
		}

		// Remove original object state
		m.state.Remove(original.Key())

		// Branches are not part of the template
		if original.Kind().IsBranch() {
			continue
		}

		// Set modified object state
		modified := modifiedRaw.(model.ObjectState)
		if err := m.state.Set(modified); err != nil {
			errors.Append(err)
		}

		replaced[original.Key().String()] = modified
	}

	// Fix list of the changed objects
	changes.Replace(func(v model.ObjectState) model.ObjectState {
		if modified, found := replaced[v.Key().String()]; found {
			return modified
		}
		return v
	})

	return errors.ErrorOrNil()
}
