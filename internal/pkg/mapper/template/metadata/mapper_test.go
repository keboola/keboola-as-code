package metadata_test

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/metadata"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func createStateWithMapper(t *testing.T, templateRef model.TemplateRef, instanceID string, objectIds metadata.ObjectIdsMap, inputsUsage *metadata.InputsUsage) (*state.State, dependencies.Mocked) {
	t.Helper()
	d := dependencies.NewMocked(t, t.Context())
	mockedState := d.MockedState()
	mockedState.Mapper().AddMapper(metadata.NewMapper(mockedState, templateRef, instanceID, objectIds, inputsUsage))
	return mockedState, d
}
