package dependencies

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/encryptionapi"
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
	loadRepositoryManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/manifest/load"
)

// public dependencies container implements Public interface.
type public struct {
	base                Base
	storageApiHost      string
	storageApiClient    client.Client
	encryptionApiClient client.Client
	stackFeatures       storageapi.FeaturesMap
	stackServices       storageapi.ServicesMap
	components          *model.ComponentsProvider
}

func NewPublicDeps(ctx context.Context, baseDeps Base, storageApiHost string) (Public, error) {
	return newPublicDeps(ctx, baseDeps, storageApiHost)
}

func newPublicDeps(ctx context.Context, baseDeps Base, storageApiHost string) (*public, error) {
	v := &public{
		base:             baseDeps,
		storageApiHost:   storageApiHost,
		storageApiClient: storageapi.ClientWithHost(baseDeps.HttpClient(), storageApiHost),
	}

	// Load API index (stack services, stack features, components)
	startTime := time.Now()
	index, err := storageapi.IndexComponentsRequest().Send(ctx, v.StorageApiPublicClient())
	if err != nil {
		return nil, err
	}
	v.base.Logger().Debugf("Storage API index loaded | %s", time.Since(startTime))

	// Set values derived from the index
	v.stackFeatures = index.Features.ToMap()
	v.stackServices = index.Services.ToMap()
	v.components = model.NewComponentsProvider(index, v.base.Logger(), v.StorageApiPublicClient())

	// Setup Encryption API
	if encryptionHost, found := v.stackServices.URLByID("encryption"); !found {
		return nil, fmt.Errorf("encryption host not found")
	} else {
		v.encryptionApiClient = encryptionapi.ClientWithHost(v.base.HttpClient(), encryptionHost.String())
	}

	return v, nil
}

func (v public) StorageApiHost() string {
	return v.storageApiHost
}

func (v public) StorageApiPublicClient() client.Sender {
	return v.storageApiClient
}

func (v public) StackFeatures() storageapi.FeaturesMap {
	return v.stackFeatures
}

func (v public) StackServices() storageapi.ServicesMap {
	return v.stackServices
}

func (v public) Components() *model.ComponentsMap {
	return v.components.Components()
}

func (v public) ComponentsProvider() *model.ComponentsProvider {
	return v.components
}

func (v public) EncryptionApiClient() client.Sender {
	return v.encryptionApiClient
}

func (v public) Template(ctx context.Context, reference model.TemplateRef) (*template.Template, error) {
	// Load repository
	repo, err := v.TemplateRepository(ctx, reference.Repository(), reference)
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

	return template.New(reference, templateRecord, versionRecord, templateDir, repo.CommonDir())
}

func (v public) TemplateRepository(ctx context.Context, reference model.TemplateRepository, forTemplate model.TemplateRef) (*repository.Repository, error) {
	// Get FS
	fs, err := v.templateRepositoryFs(ctx, reference, forTemplate)
	if err != nil {
		return nil, err
	}

	// Load manifest
	m, err := loadRepositoryManifest.Run(ctx, fs, v.base)
	if err != nil {
		return nil, err
	}
	return repository.New(reference, fs, m)
}

func (v public) templateRepositoryFs(ctx context.Context, definition model.TemplateRepository, template model.TemplateRef) (filesystem.Fs, error) {
	switch definition.Type {
	case model.RepositoryTypeDir:
		return aferofs.NewLocalFs(v.base.Logger(), definition.Url, "")
	case model.RepositoryTypeGit:
		return gitRepositoryFs(ctx, definition, template, v.base.Logger())
	default:
		panic(fmt.Errorf(`unexpected repository type "%s"`, definition.Type))
	}
}
