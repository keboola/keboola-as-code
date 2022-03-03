package metadata_test

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/metadata"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testdeps"
)

func createStateWithMapper(t *testing.T, templateRef model.TemplateRef, instanceId string) (*state.State, *testdeps.TestContainer) {
	t.Helper()
	d := testdeps.New()
	mockedState := d.EmptyState()
	mockedState.Mapper().AddMapper(metadata.NewMapper(mockedState, templateRef, instanceId))
	return mockedState, d
}
