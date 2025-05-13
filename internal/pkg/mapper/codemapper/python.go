package codemapper

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

const CustomPythonApp = "kds-team.app-custom-python"

type pythonMapper struct {
	state  *state.State
	logger log.Logger
}

func NewMapper(s *state.State) *pythonMapper {
	return &pythonMapper{state: s, logger: s.Logger()}
}

// isPythonConfig checks if the object contains python code within configuration. Can be present in `script` or `code` JSON objects.
func (m *pythonMapper) isPythonConfig(object any) (bool, error) {
	v, ok := object.(*model.Config)
	if !ok {
		return false, nil
	}

	component, err := m.state.Components().GetOrErr(v.ComponentID)
	if err != nil {
		return false, err
	}

	// Check if this is a Custom Python component
	return component.ID == CustomPythonApp, nil
}
