package dependencies

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/encryption"
	"github.com/keboola/keboola-as-code/internal/pkg/event"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func newCommonDeps(d AbstractDeps, ctx context.Context) *common {
	return &common{AbstractDeps: d, ctx: ctx}
}

type common struct {
	AbstractDeps
	ctx           context.Context
	emptyDir      filesystem.Fs
	serviceUrls   map[remote.ServiceId]remote.ServiceUrl
	storageApi    *remote.StorageApi
	encryptionApi *encryption.Api
	schedulerApi  *scheduler.Api
	eventSender   *event.Sender
	// Project
	project         *project.Project
	projectDir      filesystem.Fs
	projectManifest *project.Manifest
	projectState    *project.State
	// Template
	template         *template.Template
	templateDir      filesystem.Fs
	templateManifest *template.Manifest
	templateInputs   *template.Inputs
	templateState    *template.State
	// Template repository
	templateRepository         *repository.Repository
	templateRepositoryDir      filesystem.Fs
	templateRepositoryManifest *repository.Manifest
}

func (c *common) Ctx() context.Context {
	return c.ctx
}

func (c *common) StorageApi() (*remote.StorageApi, error) {
	if c.storageApi == nil {
		// Get host
		errors := utils.NewMultiError()
		host, err := c.StorageApiHost()
		if err != nil {
			errors.Append(err)
		}

		// Get token
		token, err := c.StorageApiToken()
		if err != nil {
			errors.Append(err)
		}

		// Validate
		if errors.Len() > 0 {
			return nil, errors
		}

		// Create API
		if api, err := remote.NewStorageApiWithToken(c.Ctx(), c.Logger(), host, token, c.ApiVerboseLogs()); err == nil {
			c.storageApi = api
		} else {
			return nil, err
		}

		// Token and manifest project ID must be same
		if c.projectManifest != nil && c.projectManifest.ProjectId() != c.storageApi.ProjectId() {
			return nil, fmt.Errorf(`given token is from the project "%d", but in manifest is defined project "%d"`, c.storageApi.ProjectId(), c.projectManifest.ProjectId())
		}
	}
	return c.storageApi, nil
}

func (c *common) EncryptionApi() (*encryption.Api, error) {
	if c.encryptionApi == nil {
		// Get Storage API
		storageApi, err := c.StorageApi()
		if err != nil {
			return nil, err
		}

		// Get Host
		host, err := c.serviceUrl(`encryption`)
		if err != nil {
			return nil, fmt.Errorf(`cannot get Encryption API host: %w`, err)
		}

		c.encryptionApi = encryption.NewEncryptionApi(c.Ctx(), c.Logger(), host, storageApi.ProjectId(), c.ApiVerboseLogs())
	}
	return c.encryptionApi, nil
}

func (c *common) SchedulerApi() (*scheduler.Api, error) {
	if c.schedulerApi == nil {
		// Get Storage API
		storageApi, err := c.StorageApi()
		if err != nil {
			return nil, err
		}

		// Get Host
		host, err := c.serviceUrl(`scheduler`)
		if err != nil {
			return nil, fmt.Errorf(`cannot get Scheduler API host: %w`, err)
		}

		c.schedulerApi = scheduler.NewSchedulerApi(c.Ctx(), c.Logger(), host, storageApi.Token().Token, c.ApiVerboseLogs())
	}
	return c.schedulerApi, nil
}

func (c *common) EventSender() (*event.Sender, error) {
	if c.eventSender == nil {
		storageApi, err := c.StorageApi()
		if err != nil {
			return nil, err
		}
		c.eventSender = event.NewSender(c.Logger(), storageApi)
	}
	return c.eventSender, nil
}

func (c *common) serviceUrl(id string) (string, error) {
	serviceUrlById, err := c.serviceUrlById()
	if err != nil {
		return "", err
	}
	if url, found := serviceUrlById[remote.ServiceId(id)]; found {
		return string(url), nil
	} else {
		return "", fmt.Errorf(`service "%s" not found`, id)
	}
}

func (c *common) serviceUrlById() (map[remote.ServiceId]remote.ServiceUrl, error) {
	if c.serviceUrls == nil {
		storageApi, err := c.StorageApi()
		if err != nil {
			return nil, err
		}
		urlById, err := storageApi.ServicesUrlById()
		if err == nil {
			c.serviceUrls = urlById
		} else {
			return nil, fmt.Errorf(`cannot load services: %w`, err)
		}
	}
	return c.serviceUrls, nil
}
