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
	keboolaPublicAPI *keboola.API
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

	v := &public{
		base:           base,
		storageAPIHost: storageAPIHost,
	}

	baseHTTPClient := base.HTTPClient()
	if c.preloadComponents {
		indexComponents, err := keboola.APIIndexWithComponents(ctx, storageAPIHost, keboola.WithClient(&baseHTTPClient))
		if err != nil {
			return nil, err
		}

		v.keboolaPublicAPI = keboola.NewAPIFromIndex(storageAPIHost, &indexComponents.Index, keboola.WithClient(&baseHTTPClient))
		v.components.Set(model.NewComponentsProvider(indexComponents, v.base.Logger(), v.keboolaPublicAPI))
	} else {
		index, err := keboola.APIIndex(ctx, storageAPIHost, keboola.WithClient(&baseHTTPClient))
		if err != nil {
			return nil, err
		}
		v.keboolaPublicAPI = keboola.NewAPIFromIndex(storageAPIHost, index, keboola.WithClient(&baseHTTPClient))
	}

	// Set values derived from the index
	v.stackFeatures = v.keboolaPublicAPI.Index().Features.ToMap()
	v.stackServices = v.keboolaPublicAPI.Index().Services.ToMap()

	return v, nil
}

func storageAPIIndexWithComponents(ctx context.Context, d Base, keboolaPublicAPI *keboola.API) (index *keboola.IndexComponents, err error) {
	startTime := time.Now()
	ctx, span := d.Tracer().Start(ctx, "kac.lib.dependencies.public.storageApiIndexWithComponents")
	span.SetAttributes(telemetry.KeepSpan())
	defer telemetry.EndSpan(span, &err)

	index, err = keboolaPublicAPI.IndexComponentsRequest().Send(ctx)
	if err != nil {
		return nil, err
	}
	d.Logger().Debugf("Storage API index with components loaded | %s", time.Since(startTime))
	return index, nil
}

func (v *public) StorageAPIHost() string {
	return v.storageAPIHost
}

func (v *public) KeboolaPublicAPI() *keboola.API {
	return v.keboolaPublicAPI
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
		index, err := storageAPIIndexWithComponents(context.Background(), v.base, v.keboolaPublicAPI)
		if err != nil {
			panic(err)
		}
		return model.NewComponentsProvider(index, v.base.Logger(), v.KeboolaPublicAPI())
	})
}

func (v *public) KeboolaProjectAPI() *keboola.API {
	return v.keboolaPublicAPI
}
