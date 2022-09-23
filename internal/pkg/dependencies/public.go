package dependencies

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/encryptionapi"
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
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
	span.SetAttributes(telemetry.KeepSpan())
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
