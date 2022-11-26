package dependencies

import (
	"context"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/encryptionapi"
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// public dependencies container implements Public interface.
type public struct {
	base                Base
	storageAPIHost      string
	storageAPIClient    client.Client
	encryptionAPIClient client.Client
	stackFeatures       storageapi.FeaturesMap
	stackServices       storageapi.ServicesMap
	components          *model.ComponentsProvider
}

func NewPublicDeps(ctx context.Context, base Base, storageAPIHost string) (v Public, err error) {
	ctx, span := base.Tracer().Start(ctx, "kac.lib.dependencies.NewPublicDeps")
	defer telemetry.EndSpan(span, &err)
	return newPublicDeps(ctx, base, storageAPIHost)
}

func newPublicDeps(ctx context.Context, base Base, storageAPIHost string) (*public, error) {
	v := &public{
		base:             base,
		storageAPIHost:   storageAPIHost,
		storageAPIClient: storageapi.ClientWithHost(base.HTTPClient(), storageAPIHost),
	}

	// Load API index (stack services, stack features, components)
	index, err := storageAPIIndex(ctx, base, v.storageAPIClient)
	if err != nil {
		return nil, err
	}

	// Set values derived from the index
	v.stackFeatures = index.Features.ToMap()
	v.stackServices = index.Services.ToMap()
	v.components = model.NewComponentsProvider(index, v.base.Logger(), v.StorageAPIPublicClient())

	// Setup Encryption API
	if encryptionHost, found := v.stackServices.URLByID("encryption"); !found {
		return nil, errors.New("encryption host not found")
	} else {
		v.encryptionAPIClient = encryptionapi.ClientWithHost(v.base.HTTPClient(), encryptionHost.String())
	}

	return v, nil
}

func storageAPIIndex(ctx context.Context, d Base, storageAPIClient client.Client) (index *storageapi.IndexComponents, err error) {
	startTime := time.Now()
	ctx, span := d.Tracer().Start(ctx, "kac.lib.dependencies.public.storageApiIndex")
	span.SetAttributes(telemetry.KeepSpan())
	defer telemetry.EndSpan(span, &err)

	index, err = storageapi.IndexComponentsRequest().Send(ctx, storageAPIClient)
	if err != nil {
		return nil, err
	}
	d.Logger().Debugf("Storage API index loaded | %s", time.Since(startTime))
	return index, nil
}

func (v public) StorageAPIHost() string {
	return v.storageAPIHost
}

func (v public) StorageAPIPublicClient() client.Sender {
	return v.storageAPIClient
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

func (v public) EncryptionAPIClient() client.Sender {
	return v.encryptionAPIClient
}
