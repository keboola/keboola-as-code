package remote_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/jarcoal/httpmock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/object"
	"github.com/keboola/keboola-as-code/internal/pkg/state/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
)

func newTestUow(t *testing.T, mappers ...interface{}) (state.UnitOfWork, *httpmock.MockTransport, *remote.State) {
	t.Helper()
	storageApi, httpTransport := testapi.NewMockedStorageApi(log.NewDebugLogger())
	mapperInst := mapper.New().AddMapper(mappers...)
	s := remote.NewState(object.NewIdSorter(), mapperInst, storageApi)
	return s.NewUnitOfWork(context.Background(), model.NoFilter(), `change desc`), httpTransport, s
}

type testMapper struct {
	remoteChanges []string
}

func (*testMapper) MapBeforeRemoteSave(recipe *model.RemoteSaveRecipe) error {
	if config, ok := recipe.Object.(*model.Config); ok {
		config.Name = "modified name"
		config.Content.Set(`key`, `api value`)
		config.Content.Set(`new`, `value`)
	}
	return nil
}

func (*testMapper) MapAfterRemoteLoad(recipe *model.RemoteLoadRecipe) error {
	if config, ok := recipe.Object.(*model.Config); ok {
		config.Name = "internal name"
		config.Content.Set(`key`, `internal value`)
		config.Content.Set(`new`, `value`)
	}
	return nil
}

func (t *testMapper) AfterRemoteOperation(changes *model.Changes) error {
	for _, objectState := range changes.Loaded() {
		t.remoteChanges = append(t.remoteChanges, fmt.Sprintf(`loaded %s`, objectState.String()))
	}
	for _, objectState := range changes.Created() {
		t.remoteChanges = append(t.remoteChanges, fmt.Sprintf(`created %s`, objectState.String()))
	}
	for _, objectState := range changes.Updated() {
		t.remoteChanges = append(t.remoteChanges, fmt.Sprintf(`updated %s`, objectState.String()))
	}
	for _, objectState := range changes.Saved() {
		t.remoteChanges = append(t.remoteChanges, fmt.Sprintf(`saved %s`, objectState.String()))
	}
	for _, objectState := range changes.Deleted() {
		t.remoteChanges = append(t.remoteChanges, fmt.Sprintf(`deleted %s`, objectState.String()))
	}
	return nil
}
