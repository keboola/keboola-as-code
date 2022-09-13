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
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
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

func NewPublicDeps(ctx context.Context, base Base, storageApiHost string) (v Public, err error) {
	ctx, span := base.Tracer().Start(ctx, "kac.lib.dependencies.NewPublicDeps")
	defer telemetry.EndSpan(span, &err)
	return newPublicDeps(ctx, base, storageApiHost)
}

func newPublicDeps(ctx context.Context, base Base, storageApiHost string) (*public, error) {
	v := &public{
		base:             base,
		storageApiHost:   storageApiHost,
		storageApiClient: storageapi.ClientWithHost(base.HttpClient(), storageApiHost),
	}

	// Load API index (stack services, stack features, components)
	index, err := storageApiIndex(ctx, base, v.storageApiClient)
	if err != nil {
		return nil, err
	}

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

func storageApiIndex(ctx context.Context, d Base, storageApiClient client.Client) (index *storageapi.IndexComponents, err error) {
	startTime := time.Now()
	ctx, span := d.Tracer().Start(ctx, "kac.lib.dependencies.public.storageApiIndex")
	defer telemetry.EndSpan(span, &err)

	index, err = storageapi.IndexComponentsRequest().Send(ctx, storageApiClient)
	if err != nil {
		return nil, err
	}
	d.Logger().Debugf("Storage API index loaded | %s", time.Since(startTime))
	return index, nil
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

func (v public) Template(ctx context.Context, reference model.TemplateRef) (tmpl *template.Template, err error) {
	ctx, span := v.base.Tracer().Start(ctx, "kac.lib.dependencies.public.Template")
	defer telemetry.EndSpan(span, &err)

	// Load repository
	repo, err := v.TemplateRepository(ctx, reference.Repository(), reference)
	if err != nil {
		return nil, err
	}

	// Get template
	templateRecord, err := repo.GetTemplateByIdOrErr(reference.TemplateId())
	if err != nil {
		return nil, err
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

func (v public) TemplateRepository(ctx context.Context, reference model.TemplateRepository, forTemplate model.TemplateRef) (repo *repository.Repository, err error) {
	ctx, span := v.base.Tracer().Start(ctx, "kac.lib.dependencies.public.TemplateRepository")
	defer telemetry.EndSpan(span, &err)

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

func (v public) templateRepositoryFs(ctx context.Context, definition model.TemplateRepository, template model.TemplateRef) (fs filesystem.Fs, err error) {
	ctx, span := v.base.Tracer().Start(ctx, "kac.lib.dependencies.public.TemplateRepository.filesystem")
	defer telemetry.EndSpan(span, &err)

	switch definition.Type {
	case model.RepositoryTypeDir:
		return aferofs.NewLocalFs(v.base.Logger(), definition.Url, "")
	case model.RepositoryTypeGit:
		return gitRepositoryFs(ctx, definition, template, v.base.Logger())
	default:
		panic(fmt.Errorf(`unexpected repository type "%s"`, definition.Type))
	}
}
