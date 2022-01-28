package replacekeys

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/template/replacekeys"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type replaceKeysMapper struct {
	state  *state.State
	logger log.Logger
	keys   replacekeys.Keys
}

func NewMapper(state *state.State, keys replacekeys.Keys) *replaceKeysMapper {
	return &replaceKeysMapper{state: state, logger: state.Logger(), keys: keys}
}

func (m *replaceKeysMapper) AfterRemoteOperation(changes *model.RemoteChanges) error {
	replacement, err := m.keys.Values()
	if err != nil {
		return err
	}

	// Replace keys in the loaded remote objects
	replaced := make(map[string]model.ObjectState)
	errors := utils.NewMultiError()
	for _, original := range changes.Loaded() {
		// Replace keys and delete original object state
		modified := replacekeys.ReplaceValues(replacement, original).(model.ObjectState)
		m.state.Remove(original.Key())

		// Branches are not part of the template
		if original.Kind().IsBranch() {
			continue
		}

		// Set modified object state
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
