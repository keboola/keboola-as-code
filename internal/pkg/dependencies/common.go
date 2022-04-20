package dependencies

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/api/client/encryptionapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/client/schedulerapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/client/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/client/storageapi/eventsender"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs/mountfs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// Abstract provides dependencies which are obtained in different in CLI and templates API.
type Abstract interface {
	Ctx() context.Context
	Logger() log.Logger
	Envs() *env.Map
	ApiVerboseLogs() bool
	StorageApiHost() (string, error)
	StorageApiToken() (string, error)
	TemplateRepository(definition model.TemplateRepository, forTemplate model.TemplateRef) (*repository.Repository, error)
}

// Common provides common dependencies for CLI and templates API.
type Common interface {
	Abstract
	Components() (*model.ComponentsMap, error)
	StorageApi() (*storageapi.Api, error)
	EncryptionApi() (*encryptionapi.Api, error)
	SchedulerApi() (*schedulerapi.Api, error)
	EventSender() (*eventsender.Sender, error)
	Template(reference model.TemplateRef) (*template.Template, error)
}

// NewCommonContainer returns dependencies container for production.
func NewCommonContainer(d Abstract) Common {
	return &commonContainer{Abstract: d}
}

type commonContainer struct {
	Abstract
	serviceUrls   map[storageapi.ServiceId]storageapi.ServiceUrl
	storageApi    *storageapi.Api
	encryptionApi *encryptionapi.Api
	schedulerApi  *schedulerapi.Api
	eventSender   *eventsender.Sender
}

func (v *commonContainer) Components() (*model.ComponentsMap, error) {
	storageApi, err := v.StorageApi()
	if err != nil {
		return nil, err
	}
	return storageApi.Components(), nil
}

func (v *commonContainer) StorageApi() (*storageapi.Api, error) {
	if v.storageApi == nil {
		// Get host
		errors := utils.NewMultiError()
		host, err := v.StorageApiHost()
		if err != nil {
			errors.Append(err)
		}

		// Get token
		token, err := v.StorageApiToken()
		if err != nil {
			errors.Append(err)
		}

		// Validate
		if errors.Len() > 0 {
			return nil, errors
		}

		// Create API
		if token == "" {
			v.storageApi = storageapi.New(v.Ctx(), v.Logger(), host, v.ApiVerboseLogs())
		} else {
			if api, err := storageapi.NewWithToken(v.Ctx(), v.Logger(), host, token, v.ApiVerboseLogs()); err == nil {
				v.storageApi = api
			} else {
				return nil, err
			}
		}
	}
	return v.storageApi, nil
}

func (v *commonContainer) EncryptionApi() (*encryptionapi.Api, error) {
	if v.encryptionApi == nil {
		// Get Storage API
		storageApi, err := v.StorageApi()
		if err != nil {
			return nil, err
		}

		// Get Host
		host, err := v.serviceUrl(`encryption`)
		if err != nil {
			return nil, fmt.Errorf(`cannot get Encryption API host: %w`, err)
		}

		v.encryptionApi = encryptionapi.New(v.Ctx(), v.Logger(), host, storageApi.ProjectId(), v.ApiVerboseLogs())
	}
	return v.encryptionApi, nil
}

func (v *commonContainer) SchedulerApi() (*schedulerapi.Api, error) {
	if v.schedulerApi == nil {
		// Get Storage API
		storageApi, err := v.StorageApi()
		if err != nil {
			return nil, err
		}

		// Get Host
		host, err := v.serviceUrl(`scheduler`)
		if err != nil {
			return nil, fmt.Errorf(`cannot get Scheduler API host: %w`, err)
		}

		v.schedulerApi = schedulerapi.New(v.Ctx(), v.Logger(), host, storageApi.Token().Token, v.ApiVerboseLogs())
	}
	return v.schedulerApi, nil
}

func (v *commonContainer) EventSender() (*eventsender.Sender, error) {
	if v.eventSender == nil {
		storageApi, err := v.StorageApi()
		if err != nil {
			return nil, err
		}
		v.eventSender = eventsender.New(v.Logger(), storageApi)
	}
	return v.eventSender, nil
}

func (v *commonContainer) serviceUrl(id string) (string, error) {
	serviceUrlById, err := v.serviceUrlById()
	if err != nil {
		return "", err
	}
	if url, found := serviceUrlById[storageapi.ServiceId(id)]; found {
		return string(url), nil
	} else {
		return "", fmt.Errorf(`service "%s" not found`, id)
	}
}

func (v *commonContainer) serviceUrlById() (map[storageapi.ServiceId]storageapi.ServiceUrl, error) {
	if v.serviceUrls == nil {
		storageApi, err := v.StorageApi()
		if err != nil {
			return nil, err
		}
		urlById, err := storageApi.ServicesUrlById()
		if err == nil {
			v.serviceUrls = urlById
		} else {
			return nil, fmt.Errorf(`cannot load services: %w`, err)
		}
	}
	return v.serviceUrls, nil
}

func (v *commonContainer) Template(reference model.TemplateRef) (*template.Template, error) {
	// Load repository
	repo, err := v.TemplateRepository(reference.Repository(), reference)
	if err != nil {
		return nil, err
	}

	// Get template version
	templateRecord, versionRecord, err := repo.GetTemplateVersion(reference.TemplateId(), reference.Version())
	if err != nil {
		return nil, err
	}

	// Check if template dir exists
	templatePath := versionRecord.Path()
	if !repo.Fs().IsDir(templatePath) {
		return nil, fmt.Errorf(`template dir "%s" not found`, templatePath)
	}

	// Template dir
	rootFs, err := repo.Fs().SubDirFs(templatePath)
	if err != nil {
		return nil, err
	}

	// Mount "_common" dir from the repository to "<common>"
	mountPath := filesystem.Join(template.SrcDirectory, repository.CommonDirectoryMountPoint)
	mountPoint := mountfs.NewMountPoint(mountPath, repo.CommonDir())
	fs, err := aferofs.NewMountFs(rootFs, mountPoint)
	if err != nil {
		return nil, err
	}

	return template.New(reference, templateRecord, versionRecord, fs, v)
}
