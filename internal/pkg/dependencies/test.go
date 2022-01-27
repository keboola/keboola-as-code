package dependencies

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/api/encryptionapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/schedulerapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi/eventsender"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
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

func (c *TestContainer) SetProjectState(state *project.State) {
	c.projectState = state
}

func (c *TestContainer) SetProjectManifest(manifest *project.Manifest) {
	c.projectDir = c.Fs()
	c.projectManifest = manifest
}

func (c *TestContainer) SetTemplateState(state *template.State) {
	c.templateState = state
}

func (c *TestContainer) SetTemplateManifest(manifest *template.Manifest) {
	c.templateDir = c.Fs()
	c.templateManifest = manifest
}
