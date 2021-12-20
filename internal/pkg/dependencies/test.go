package dependencies

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/encryption"
	"github.com/keboola/keboola-as-code/internal/pkg/event"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

// TestContainer for use in tests. It allows modification of the values.
type TestContainer struct {
	*common
}

func (c *TestContainer) SetCtx(ctx context.Context) {
	c.ctx = ctx
}

func (c *TestContainer) SetStorageApi(api *remote.StorageApi) {
	c.storageApi = api
}

func (c *TestContainer) SetEncryptionApi(api *encryption.Api) {
	c.encryptionApi = api
}

func (c *TestContainer) SetSchedulerApi(api *scheduler.Api) {
	c.schedulerApi = api
}

func (c *TestContainer) EventSender(sender *event.Sender) {
	c.eventSender = sender
}

func (c *TestContainer) SetProjectState(state *project.State) {
	c.projectState = state
}

func (c *TestContainer) SetTemplateState(state *template.State) {
	c.templateState = state
}
