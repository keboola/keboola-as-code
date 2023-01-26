package dependencies

import (
	"context"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/keboola"

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
	stackFeatures       keboola.FeaturesMap
	stackServices       keboola.ServicesMap
	components          Lazy[*model.ComponentsProvider]
}

type PublicDepsOption func(*publicDepsConfig)

type publicDepsConfig struct {
	preloadComponents bool
}

func publicDepsDefaultConfig() publicDepsConfig {
	return publicDepsConfig{preloadComponents: false}
}

// WithPreloadComponents defines if the components should be retrieved from Storage index on startup.
func WithPreloadComponents(v bool) PublicDepsOption {
	return func(c *publicDepsConfig) {
		c.preloadComponents = v
	}
}

func NewPublicDeps(ctx context.Context, base Base, storageAPIHost string, opts ...PublicDepsOption) (v Public, err error) {
	ctx, span := base.Tracer().Start(ctx, "kac.lib.dependencies.NewPublicDeps")
	defer telemetry.EndSpan(span, &err)
	return newPublicDeps(ctx, base, storageAPIHost, opts...)
}

func newPublicDeps(ctx context.Context, base Base, storageAPIHost string, opts ...PublicDepsOption) (*public, error) {
	// Apply options
	c := publicDepsDefaultConfig()
	for _, o := range opts {
		o(&c)
	}

	v := &public{
		base:             base,
		storageAPIHost:   storageAPIHost,
		storageAPIClient: keboola.ClientWithHost(base.HTTPClient(), storageAPIHost),
	}

	// Load API index (stack services, stack features, components)
	var index *keboola.Index
	if c.preloadComponents {
		indexWithComponents, err := storageAPIIndexWithComponents(ctx, base, v.storageAPIClient)
		if err != nil {
			return nil, err
		}
		v.components.Set(model.NewComponentsProvider(indexWithComponents, v.base.Logger(), v.StorageAPIPublicClient()))
		index = &indexWithComponents.Index
	} else {
		idx, err := storageAPIIndex(ctx, base, v.storageAPIClient)
		if err != nil {
			return nil, err
		}
		index = idx
	}

	// Set values derived from the index
	v.stackFeatures = index.Features.ToMap()
	v.stackServices = index.Services.ToMap()

	// Setup Encryption API
	if encryptionHost, found := v.stackServices.URLByID("encryption"); !found {
		return nil, errors.New("encryption host not found")
	} else {
		v.encryptionAPIClient = keboola.ClientWithHost(v.base.HTTPClient(), encryptionHost.String())
	}

	return v, nil
}

func storageAPIIndexWithComponents(ctx context.Context, d Base, storageAPIClient client.Client) (index *keboola.IndexComponents, err error) {
	startTime := time.Now()
	ctx, span := d.Tracer().Start(ctx, "kac.lib.dependencies.public.storageApiIndexWithComponents")
	span.SetAttributes(telemetry.KeepSpan())
	defer telemetry.EndSpan(span, &err)

	index, err = keboola.IndexComponentsRequest().Send(ctx, storageAPIClient)
	if err != nil {
		return nil, err
	}
	d.Logger().Debugf("Storage API index with components loaded | %s", time.Since(startTime))
	return index, nil
}

func storageAPIIndex(ctx context.Context, d Base, storageAPIClient client.Client) (index *keboola.Index, err error) {
	startTime := time.Now()
	ctx, span := d.Tracer().Start(ctx, "kac.lib.dependencies.public.storageApiIndex")
	span.SetAttributes(telemetry.KeepSpan())
	defer telemetry.EndSpan(span, &err)

	index, err = keboola.IndexRequest().Send(ctx, storageAPIClient)
	if err != nil {
		return nil, err
	}
	d.Logger().Debugf("Storage API index loaded | %s", time.Since(startTime))
	return index, nil
}

func (v *public) StorageAPIHost() string {
	return v.storageAPIHost
}

func (v *public) StorageAPIPublicClient() client.Sender {
	return v.storageAPIClient
}

func (v *public) StackFeatures() keboola.FeaturesMap {
	return v.stackFeatures
}

func (v *public) StackServices() keboola.ServicesMap {
	return v.stackServices
}

func (v *public) Components() *model.ComponentsMap {
	return v.ComponentsProvider().Components()
}

func (v *public) ComponentsProvider() *model.ComponentsProvider {
	return v.components.MustInitAndGet(func() *model.ComponentsProvider {
		index, err := storageAPIIndexWithComponents(context.Background(), v.base, v.storageAPIClient)
		if err != nil {
			panic(err)
		}
		return model.NewComponentsProvider(index, v.base.Logger(), v.StorageAPIPublicClient())
	})
}

func (v *public) EncryptionAPIClient() client.Sender {
	return v.encryptionAPIClient
}
