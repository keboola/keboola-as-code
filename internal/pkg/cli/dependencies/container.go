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
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	createManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/local/manifest/create"
	loadManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/local/manifest/load"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

var (
	ErrMissingStorageApiHost  = fmt.Errorf(`missing Storage API host`)
	ErrMissingStorageApiToken = fmt.Errorf(`missing Storage API token`)
	ErrMetadataDirNotFound    = fmt.Errorf("metadata directory not found")
	ErrMetadataDirFound       = fmt.Errorf("metadata directory not expected, but found")
)

type Container struct {
	ctx              context.Context
	envs             *env.Map
	fs               filesystem.Fs
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

func (c *Container) Fs() filesystem.Fs {
	return c.fs
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
			return nil, fmt.Errorf(`given token is from the project "%d", but in manifest is defined priject "%d"`, c.storageApi.ProjectId(), c.manifest.Project.Id)
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

func (c *Container) AssertMetaDirExists() error {
	if !c.Fs().IsDir(filesystem.MetadataDir) {
		return ErrMetadataDirNotFound
	}
	return nil
}

func (c *Container) AssertMetaDirNotExists() error {
	if c.Fs().Exists(filesystem.MetadataDir) {
		return ErrMetadataDirFound
	}
	return nil
}

func (c *Container) CreateManifest(o createManifest.Options) (*manifest.Manifest, error) {
	if m, err := createManifest.Run(o, c); err == nil {
		c.manifest = m
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
