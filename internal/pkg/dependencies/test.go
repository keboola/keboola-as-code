package dependencies

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/api/encryptionapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/schedulerapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi/eventsender"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
)

// TestContainer for use in tests. It allows modification of the values.
type TestContainer struct {
	*common
}

func (c *TestContainer) SetCtx(ctx context.Context) {
	c.ctx = ctx
}

func (c *TestContainer) SetStorageApi(api *storageapi.Api) {
	c.storageApi = api
}

func (c *TestContainer) SetEncryptionApi(api *encryptionapi.Api) {
	c.encryptionApi = api
}

func (c *TestContainer) SetSchedulerApi(api *schedulerapi.Api) {
	c.schedulerApi = api
}

func (c *TestContainer) EventSender(sender *eventsender.Sender) {
	c.eventSender = sender
}

func (c *TestContainer) SetProject(prj *project.Project) {
	c.project = prj
}

func (c *TestContainer) SetProjectState(state *project.State) {
	c.projectState = state
}
