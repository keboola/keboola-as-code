package local_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
)

func newTestUow(t *testing.T, mappers ...interface{}) (*local.State, state.UnitOfWork, filesystem.Fs, manifest.Manifest) {
	t.Helper()
	fs := testfs.NewMemoryFs()
	manifestInst := manifest.NewInMemory()
	s, uow := newTestUowFor(t, fs, manifestInst, mappers...)
	return s, uow, fs, manifestInst
}

func newTestUowFor(t *testing.T, fs filesystem.Fs, manifestInst manifest.Manifest, mappers ...interface{}) (*local.State, state.UnitOfWork) {
	t.Helper()
	components := model.NewComponentsMap(testapi.NewMockedComponentsProvider())
	mapperInst := mapper.New().AddMapper(mappers...)
	s, err := local.NewState(log.NewNopLogger(), fs, manifestInst, components, mapperInst)
	assert.NoError(t, err)
	return s, s.NewUnitOfWork(context.Background(), manifestInst.Filter())
}

type testMapper struct {
	localChanges []string
}

func (*testMapper) MapBeforeLocalSave(recipe *model.LocalSaveRecipe) error {
	if config, ok := recipe.Object.(*model.Config); ok {
		config.Name = "modified name"
		config.Content.Set(`key`, `local value`)
		config.Content.Set(`new`, `value`)
	}
	return nil
}

func (*testMapper) MapAfterLocalLoad(recipe *model.LocalLoadRecipe) error {
	if config, ok := recipe.Object.(*model.Config); ok {
		config.Name = "internal name"
		config.Content.Set(`key`, `internal value`)
		config.Content.Set(`new`, `value`)
	}
	return nil
}

func (t *testMapper) AfterLocalOperation(changes *model.Changes) error {
	for _, objectState := range changes.Loaded() {
		t.localChanges = append(t.localChanges, fmt.Sprintf(`loaded %s`, objectState.String()))
	}
	for _, objectState := range changes.Created() {
		t.localChanges = append(t.localChanges, fmt.Sprintf(`created %s`, objectState.String()))
	}
	for _, objectState := range changes.Updated() {
		t.localChanges = append(t.localChanges, fmt.Sprintf(`updated %s`, objectState.String()))
	}
	for _, objectState := range changes.Saved() {
		t.localChanges = append(t.localChanges, fmt.Sprintf(`saved %s`, objectState.String()))
	}
	for _, objectState := range changes.Deleted() {
		t.localChanges = append(t.localChanges, fmt.Sprintf(`deleted %s`, objectState.String()))
	}
	return nil
}
