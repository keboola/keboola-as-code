package dependencies

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/encryptionapi"
	"github.com/keboola/go-client/pkg/schedulerapi"
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/event"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// Abstract provides dependencies which are obtained in different in CLI and templates API.
type Abstract interface {
	Logger() log.Logger
	Envs() *env.Map
	HttpClient() client.Client
	StorageApiHost() (string, error)
	StorageApiToken() (string, error)
	TemplateRepository(definition model.TemplateRepository, forTemplate model.TemplateRef) (*repository.Repository, error)
}

// Common provides common dependencies for CLI and templates API.
type Common interface {
	Abstract
	ProjectID() (int, error)
	StorageAPITokenID() (string, error)
	StorageApiClient() (client.Sender, error)
	EncryptionApiClient() (client.Sender, error)
	SchedulerApiClient() (client.Sender, error)
	Features() (storageapi.FeaturesMap, error)
	Components() (model.ComponentsMap, error)
	EventSender() (event.Sender, error)
	Template(reference model.TemplateRef) (*template.Template, error)
}

// NewCommonContainer returns dependencies container for production.
func NewCommonContainer(ctx context.Context, d Abstract) *CommonContainer {
	return &CommonContainer{
		Abstract:        d,
		ctx:             ctx,
		storageApi:      new(Lazy[clientWithToken]),
		storageApiIndex: new(Lazy[storageapi.Index]),
		services:        new(Lazy[storageapi.ServicesMap]),
		features:        new(Lazy[storageapi.FeaturesMap]),
		encryptionApi:   new(Lazy[client.Client]),
		schedulerApi:    new(Lazy[client.Client]),
		components:      new(Lazy[model.ComponentsMap]),
		eventSender:     new(Lazy[event.Sender]),
	}
}

type CommonContainer struct {
	Abstract
	ctx             context.Context
	storageApi      *Lazy[clientWithToken]
	storageApiIndex *Lazy[storageapi.Index]
	services        *Lazy[storageapi.ServicesMap]
	features        *Lazy[storageapi.FeaturesMap]
	encryptionApi   *Lazy[client.Client]
	schedulerApi    *Lazy[client.Client]
	components      *Lazy[model.ComponentsMap]
	eventSender     *Lazy[event.Sender]
}

// clientWithToken is client.Client with information about the authenticated project.
type clientWithToken struct {
	client.Client
	Token *storageapi.Token
}

func (v *CommonContainer) WithStorageApiClient(client client.Client, token *storageapi.Token) *CommonContainer {
	clone := *v
	clone.storageApi = new(Lazy[clientWithToken])
	clone.storageApi.Set(clientWithToken{Client: client, Token: token})
	return &clone
}

func (v *CommonContainer) ProjectID() (int, error) {
	storageApi, err := v.getStorageApi()
	if err != nil {
		return 0, err
	}
	if storageApi.Token == nil {
		return 0, fmt.Errorf("cannot get project ID: unauthenticated")
	}
	return storageApi.Token.Owner.ID, nil
}

func (v *CommonContainer) StorageAPITokenID() (string, error) {
	storageApi, err := v.getStorageApi()
	if err != nil {
		return "", err
	}
	if storageApi.Token == nil {
		return "", fmt.Errorf("cannot get token ID: unauthenticated")
	}
	return storageApi.Token.ID, nil
}

func (v *CommonContainer) StorageApiClient() (client.Sender, error) {
	if c, err := v.getStorageApi(); err == nil {
		return c.Client, nil
	} else {
		return nil, err
	}
}

func (v *CommonContainer) getStorageApi() (clientWithToken, error) {
	return v.storageApi.InitAndGet(func() (*clientWithToken, error) {
		// Get host
		errors := utils.NewMultiError()
		host, err := v.StorageApiHost()
		if err != nil {
			errors.Append(err)
		}

		// Get token
		tokenStr, err := v.StorageApiToken()
		if err != nil {
			errors.Append(err)
		}

		// Validate
		if errors.Len() > 0 {
			return nil, errors
		}

		// Create API client
		c := storageapi.ClientWithHost(v.HttpClient(), host)
		api := &clientWithToken{Client: c}

		// Verify token
		if tokenStr != "" {
			api.Client = storageapi.ClientWithToken(c, tokenStr)
			if token, err := storageapi.VerifyTokenRequest(tokenStr).Send(v.ctx, api.Client); err == nil {
				api.Token = token
				v.Logger().Debugf("Storage API token is valid.")
				v.Logger().Debugf(`Project id: "%d", project name: "%s".`, token.ProjectID(), token.ProjectName())
			} else {
				return nil, fmt.Errorf("the specified storage API token is not valid")
			}
		}

		return api, nil
	})
}

func (v *CommonContainer) getServices() (storageapi.ServicesMap, error) {
	return v.services.InitAndGet(func() (*storageapi.ServicesMap, error) {
		if index, err := v.getStorageIndex(); err == nil {
			services := index.Services.ToMap()
			return &services, nil
		} else {
			return nil, err
		}
	})
}

func (v *CommonContainer) getStorageIndex() (storageapi.Index, error) {
	return v.storageApiIndex.InitAndGet(func() (*storageapi.Index, error) {
		// Get Storage API
		c, err := v.StorageApiClient()
		if err != nil {
			return nil, err
		}

		// Get components index
		return storageapi.IndexRequest().Send(v.ctx, c)
	})
}

func (v *CommonContainer) Features() (storageapi.FeaturesMap, error) {
	return v.features.InitAndGet(func() (*storageapi.FeaturesMap, error) {
		if index, err := v.getStorageIndex(); err == nil {
			features := index.Features.ToMap()
			return &features, nil
		} else {
			return nil, err
		}
	})
}

func (v *CommonContainer) EncryptionApiClient() (client.Sender, error) {
	return v.encryptionApi.InitAndGet(func() (*client.Client, error) {
		// Get services
		services, err := v.getServices()
		if err != nil {
			return nil, err
		}

		// Get host
		host, found := services.URLByID("encryption")
		if !found {
			return nil, fmt.Errorf("encryption host not found")
		}

		// Create API client
		c := encryptionapi.ClientWithHost(v.HttpClient(), host.String())
		return &c, nil
	})
}

func (v *CommonContainer) SchedulerApiClient() (client.Sender, error) {
	return v.schedulerApi.InitAndGet(func() (*client.Client, error) {
		// Get token
		x, err := v.getStorageApi()
		if err != nil {
			return nil, err
		}

		// Get services
		services, err := v.getServices()
		if err != nil {
			return nil, err
		}

		// Get host
		host, found := services.URLByID("scheduler")
		if !found {
			return nil, fmt.Errorf("scheduler host not found")
		}

		// Create API client
		c := schedulerapi.ClientWithHostAndToken(v.HttpClient(), host.String(), x.Token.Token)
		return &c, nil
	})
}

func (v *CommonContainer) Components() (model.ComponentsMap, error) {
	return v.components.InitAndGet(func() (*model.ComponentsMap, error) {
		// Get Storage API
		c, err := v.StorageApiClient()
		if err != nil {
			return nil, err
		}

		// Get components index
		if index, err := storageapi.IndexComponentsRequest().Send(v.ctx, c); err == nil {
			v := model.NewComponentsMap(index.Components)
			return &v, nil
		} else {
			return nil, err
		}
	})
}

func (v *CommonContainer) EventSender() (event.Sender, error) {
	return v.eventSender.InitAndGet(func() (*event.Sender, error) {
		// Get Storage API
		c, err := v.getStorageApi()
		if err != nil {
			return nil, err
		}
		return event.NewSender(v.Logger(), c.Client, c.Token.ProjectID()), nil
	})
}

func (v *CommonContainer) Template(reference model.TemplateRef) (*template.Template, error) {
	// Load repository
	repo, err := v.TemplateRepository(reference.Repository(), reference)
	if err != nil {
		return nil, err
	}

	// Get template
	templateRecord, found := repo.GetTemplateById(reference.TemplateId())
	if !found {
		return nil, manifest.TemplateNotFoundError{}
	}

	// Get template version
	versionRecord, err := templateRecord.GetVersionOrErr(reference.Version())
	if err != nil {
		return nil, err
	}

	// Check if template dir exists
	templatePath := versionRecord.Path()
	if !repo.Fs().IsDir(templatePath) {
		return nil, fmt.Errorf(`template dir "%s" not found`, templatePath)
	}

	// Template dir
	templateDir, err := repo.Fs().SubDirFs(templatePath)
	if err != nil {
		return nil, err
	}

	// Update sem version in reference
	reference = model.NewTemplateRef(reference.Repository(), reference.TemplateId(), versionRecord.Version.String())

	return template.New(reference, templateRecord, versionRecord, templateDir, repo.CommonDir(), v)
}

func (v *CommonContainer) Clone() *CommonContainer {
	clone := *v
	return &clone
}
