package dependencies

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/api/encryptionapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/schedulerapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi/eventsender"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func newCommonDeps(d AbstractDeps, ctx context.Context) *common {
	return &common{AbstractDeps: d, ctx: ctx}
}

type common struct {
	AbstractDeps
	ctx           context.Context
	emptyDir      filesystem.Fs
	serviceUrls   map[storageapi.ServiceId]storageapi.ServiceUrl
	storageApi    *storageapi.Api
	encryptionApi *encryptionapi.Api
	schedulerApi  *schedulerapi.Api
	eventSender   *eventsender.Sender
	// Project
	project      *project.Project
	projectDir   filesystem.Fs
	projectState *project.State
}

func (c *common) Ctx() context.Context {
	return c.ctx
}

func (c *common) Components() (*model.ComponentsMap, error) {
	storageApi, err := c.StorageApi()
	if err != nil {
		return nil, err
	}
	return storageApi.Components(), nil
}

func (c *common) StorageApi() (*storageapi.Api, error) {
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
		if api, err := storageapi.NewWithToken(c.Ctx(), c.Logger(), host, token, c.ApiVerboseLogs()); err == nil {
			c.storageApi = api
		} else {
			return nil, err
		}

		// Token and manifest project ID must be same
		if c.LocalProjectExists() {
			prj, err := c.LocalProject(false)
			if err != nil {
				return nil, err
			}
			projectManifest := prj.ProjectManifest()
			if projectManifest != nil && projectManifest.ProjectId() != c.storageApi.ProjectId() {
				return nil, fmt.Errorf(`given token is from the project "%d", but in manifest is defined project "%d"`, c.storageApi.ProjectId(), projectManifest.ProjectId())
			}
		}
	}
	return c.storageApi, nil
}

func (c *common) EncryptionApi() (*encryptionapi.Api, error) {
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

		c.encryptionApi = encryptionapi.New(c.Ctx(), c.Logger(), host, storageApi.ProjectId(), c.ApiVerboseLogs())
	}
	return c.encryptionApi, nil
}

func (c *common) SchedulerApi() (*schedulerapi.Api, error) {
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

		c.schedulerApi = schedulerapi.New(c.Ctx(), c.Logger(), host, storageApi.Token().Token, c.ApiVerboseLogs())
	}
	return c.schedulerApi, nil
}

func (c *common) EventSender() (*eventsender.Sender, error) {
	if c.eventSender == nil {
		storageApi, err := c.StorageApi()
		if err != nil {
			return nil, err
		}
		c.eventSender = eventsender.New(c.Logger(), storageApi)
	}
	return c.eventSender, nil
}

func (c *common) serviceUrl(id string) (string, error) {
	serviceUrlById, err := c.serviceUrlById()
	if err != nil {
		return "", err
	}
	if url, found := serviceUrlById[storageapi.ServiceId(id)]; found {
		return string(url), nil
	} else {
		return "", fmt.Errorf(`service "%s" not found`, id)
	}
}

func (c *common) serviceUrlById() (map[storageapi.ServiceId]storageapi.ServiceUrl, error) {
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
