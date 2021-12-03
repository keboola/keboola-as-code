package dependencies

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/encryption"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/event"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	createManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/local/manifest/create"
	loadManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/local/manifest/load"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

var (
	ErrMissingStorageApiHost          = fmt.Errorf(`missing Storage API host`)
	ErrMissingStorageApiToken         = fmt.Errorf(`missing Storage API token`)
	ErrProjectManifestNotFound        = fmt.Errorf("project manifest not found")
	ErrRepoManifestNotFound           = fmt.Errorf("repository manifest not found")
	ErrExpectedProjectFoundRepository = fmt.Errorf("project manifest not found, found repository manifest")
	ErrExpectedRepositoryFoundProject = fmt.Errorf("repository manifest not found, found project manifest")
	ErrProjectDirFound                = fmt.Errorf("project directory not expected, but found")
	ErrRepoDirFound                   = fmt.Errorf("repository directory not expected, but found")
	ErrDirIsNotEmpty                  = fmt.Errorf("dir is not empty")
)

type Container struct {
	ctx              context.Context
	envs             *env.Map
	fs               filesystem.Fs
	emptyDir         filesystem.Fs
	projectDir       filesystem.Fs
	repositoryDir    filesystem.Fs
	dialogs          *dialog.Dialogs
	logger           *zap.SugaredLogger
	options          *options.Options
	hostFromManifest bool // load Storage Api host from manifest
	serviceUrls      map[remote.ServiceId]remote.ServiceUrl
	storageApi       *remote.StorageApi
	encryptionApi    *encryption.Api
	schedulerApi     *scheduler.Api
	eventSender      *event.Sender
	manifest         *manifest.Manifest
	state            *state.State
}

func NewContainer(ctx context.Context, envs *env.Map, fs filesystem.Fs, dialogs *dialog.Dialogs, logger *zap.SugaredLogger, options *options.Options) *Container {
	c := &Container{}
	c.ctx = ctx
	c.envs = envs
	c.fs = fs
	c.dialogs = dialogs
	c.logger = logger
	c.options = options
	return c
}

func (c *Container) Ctx() context.Context {
	return c.ctx
}

func (c *Container) Envs() *env.Map {
	return c.envs
}

func (c *Container) BasePath() string {
	return c.fs.BasePath()
}

func (c *Container) ProjectManifestExists() bool {
	path := filesystem.Join(filesystem.MetadataDir, manifest.FileName)
	return c.fs.IsFile(path)
}

func (c *Container) RepositoryManifestExists() bool {
	path := filesystem.Join(filesystem.MetadataDir, repository.FileName)
	return c.fs.IsFile(path)
}

func (c *Container) EmptyDir() (filesystem.Fs, error) {
	if c.emptyDir == nil {
		// Project dir is not expected
		if c.ProjectManifestExists() {
			return nil, ErrProjectDirFound
		}

		// Repository dir is not expected
		if c.RepositoryManifestExists() {
			return nil, ErrRepoDirFound
		}

		// Read directory
		items, err := c.fs.ReadDir(`.`)
		if err != nil {
			return nil, err
		}

		// Filter out ignored files
		found := utils.NewMultiError()
		for _, item := range items {
			if !filesystem.IsIgnoredPath(item.Name(), item) {
				path := item.Name()
				if found.Len() > 5 {
					found.Append(fmt.Errorf(path + ` ...`))
					break
				} else {
					found.Append(fmt.Errorf(path))
				}
			}
		}

		// Directory must be empty
		if found.Len() > 0 {
			return nil, utils.PrefixError(fmt.Sprintf(`directory "%s" it not empty, found`, c.fs.BasePath()), found)
		}

		c.emptyDir = c.fs
	}

	return c.emptyDir, nil
}

func (c *Container) ProjectDir() (filesystem.Fs, error) {
	if c.projectDir == nil {
		if !c.ProjectManifestExists() {
			if c.RepositoryManifestExists() {
				return nil, ErrExpectedProjectFoundRepository
			}
			return nil, ErrProjectManifestNotFound
		}

		c.projectDir = c.fs
	}
	return c.projectDir, nil
}

func (c *Container) RepositoryDir() (filesystem.Fs, error) {
	if c.repositoryDir == nil {
		if !c.RepositoryManifestExists() {
			if c.ProjectManifestExists() {
				return nil, ErrExpectedRepositoryFoundProject
			}
			return nil, ErrRepoManifestNotFound
		}

		c.repositoryDir = c.fs
	}
	return c.repositoryDir, nil
}

func (c *Container) Dialogs() *dialog.Dialogs {
	return c.dialogs
}

func (c *Container) Logger() *zap.SugaredLogger {
	return c.logger
}

func (c *Container) Options() *options.Options {
	return c.options
}

func (c *Container) SetStorageApiHost(host string) {
	c.options.Set(`storage-api-host`, host)
}

func (c *Container) SetStorageApiToken(host string) {
	c.options.Set(`storage-api-token`, host)
}

func (c *Container) LoadStorageApiHostFromManifest() {
	c.hostFromManifest = true
}

func (c *Container) StorageApi() (*remote.StorageApi, error) {
	if c.storageApi == nil {
		// Get host
		var host string
		if c.hostFromManifest {
			if m, err := c.Manifest(); err == nil {
				host = m.Project.ApiHost
			} else {
				return nil, err
			}
		} else {
			host = c.options.GetString(options.StorageApiHostOpt)
		}

		// Get token
		token := c.options.GetString(options.StorageApiTokenOpt)

		// Validate
		errors := utils.NewMultiError()
		if host == "" {
			errors.Append(ErrMissingStorageApiHost)
		}
		if token == "" {
			errors.Append(ErrMissingStorageApiToken)
		}
		if errors.Len() > 0 {
			return nil, errors
		}

		// Create API
		if api, err := remote.NewStorageApiWithToken(c.Ctx(), c.Logger(), host, token, c.options.VerboseApi); err == nil {
			c.storageApi = api
		} else {
			return nil, err
		}

		// Token and manifest project ID must be same
		if c.manifest != nil && c.manifest.Project.Id != c.storageApi.ProjectId() {
			return nil, fmt.Errorf(`given token is from the project "%d", but in manifest is defined project "%d"`, c.storageApi.ProjectId(), c.manifest.Project.Id)
		}
	}
	return c.storageApi, nil
}

func (c *Container) EncryptionApi() (*encryption.Api, error) {
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

		c.encryptionApi = encryption.NewEncryptionApi(c.Ctx(), c.Logger(), host, storageApi.ProjectId(), c.options.VerboseApi)
	}
	return c.encryptionApi, nil
}

func (c *Container) SchedulerApi() (*scheduler.Api, error) {
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

		c.schedulerApi = scheduler.NewSchedulerApi(c.Ctx(), c.Logger(), host, storageApi.Token().Token, c.options.VerboseApi)
	}
	return c.schedulerApi, nil
}

func (c *Container) EventSender() (*event.Sender, error) {
	if c.eventSender == nil {
		storageApi, err := c.StorageApi()
		if err != nil {
			return nil, err
		}
		c.eventSender = event.NewSender(c.Logger(), storageApi)
	}
	return c.eventSender, nil
}

func (c *Container) CreateManifest(o createManifest.Options) (*manifest.Manifest, error) {
	if m, err := createManifest.Run(o, c); err == nil {
		c.manifest = m
		c.projectDir = c.fs
		c.emptyDir = nil
		return m, nil
	} else {
		return nil, fmt.Errorf(`cannot create manifest: %w`, err)
	}
}

func (c *Container) Manifest() (*manifest.Manifest, error) {
	if c.manifest == nil {
		if m, err := loadManifest.Run(c); err == nil {
			c.manifest = m
		} else {
			return nil, err
		}
	}
	return c.manifest, nil
}

func (c *Container) LoadStateOnce(loadOptions loadState.Options) (*state.State, error) {
	if c.state == nil {
		if s, err := loadState.Run(loadOptions, c); err == nil {
			c.state = s
		} else {
			return nil, err
		}
	}
	return c.state, nil
}

func (c *Container) serviceUrl(id string) (string, error) {
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

func (c *Container) serviceUrlById() (map[remote.ServiceId]remote.ServiceUrl, error) {
	if c.serviceUrls == nil {
		storageApi, err := c.StorageApi()
		if err != nil {
			return nil, err
		}
		urlById, err := storageApi.ServicesUrlById()
		if err == nil {
			c.serviceUrls = urlById
		} else {
			return nil, err
		}
	}
	return c.serviceUrls, nil
}
