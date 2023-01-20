package dependencies

import (
	"context"
	"time"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// public dependencies container implements Public interface.
type public struct {
	base             Base
	components       Lazy[*model.ComponentsProvider]
	keboolaAPIClient *keboola.API
	stackFeatures    keboola.FeaturesMap
	stackServices    keboola.ServicesMap
	storageAPIHost   string
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

	baseHTTPClient := base.HTTPClient()
	v := &public{
		base:             base,
		storageAPIHost:   storageAPIHost,
		keboolaAPIClient: keboola.NewAPI(ctx, storageAPIHost, keboola.WithClient(&baseHTTPClient)),
	}
	// Set values derived from the index
	v.stackFeatures = v.keboolaAPIClient.Features().ToMap()
	v.stackServices = v.keboolaAPIClient.Services().ToMap()

	if c.preloadComponents {
		indexWithComponents, err := storageAPIIndexWithComponents(ctx, base, v.keboolaAPIClient)
		if err != nil {
			return nil, err
		}
		v.components.Set(model.NewComponentsProvider(indexWithComponents, v.base.Logger(), v.KeboolaAPIPublicClient()))
	}

	return v, nil
}

func storageAPIIndexWithComponents(ctx context.Context, d Base, keboolaAPIClient *keboola.API) (index *keboola.IndexComponents, err error) {
	startTime := time.Now()
	ctx, span := d.Tracer().Start(ctx, "kac.lib.dependencies.public.storageApiIndexWithComponents")
	span.SetAttributes(telemetry.KeepSpan())
	defer telemetry.EndSpan(span, &err)

	index, err = keboolaAPIClient.IndexComponentsRequest().Send(ctx)
	if err != nil {
		return nil, err
	}
	d.Logger().Debugf("Storage API index with components loaded | %s", time.Since(startTime))
	return index, nil
}

func (v *public) StorageAPIHost() string {
	return v.storageAPIHost
}

func (v *public) KeboolaAPIPublicClient() *keboola.API {
	return v.keboolaAPIClient
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
		index, err := storageAPIIndexWithComponents(context.Background(), v.base, v.keboolaAPIClient)
		if err != nil {
			panic(err)
		}
		return model.NewComponentsProvider(index, v.base.Logger(), v.KeboolaAPIPublicClient())
	})
}

func (v *public) KeboolaAPIClient() *keboola.API {
	return v.keboolaAPIClient
}
