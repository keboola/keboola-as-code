package replacekeys

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type replaceKeysMapper struct {
	state  *state.State
	logger log.Logger
	keys   KeysReplacement
}

func NewMapper(state *state.State, keys KeysReplacement) *replaceKeysMapper {
	return &replaceKeysMapper{state: state, logger: state.Logger(), keys: keys}
}

func (m *replaceKeysMapper) OnRemoteChange(changes *model.RemoteChanges) error {
	replacement, err := m.keys.Values()
	if err != nil {
		return err
	}

	errors := utils.NewMultiError()
	for _, original := range changes.Loaded() {
		modified := replaceValues(replacement, original).(model.ObjectState)
		m.state.Delete(original.Key())
		if err := m.state.Set(modified); err != nil {
			errors.Append(err)
		}
	}

	return errors.ErrorOrNil()
}
