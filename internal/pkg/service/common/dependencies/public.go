package dependencies

import (
	"context"
	"time"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// publicScope dependencies container implements PublicScope interface.
type publicScope struct {
	base             BaseScope
	components       Lazy[*model.ComponentsProvider]
	keboolaPublicAPI *keboola.PublicAPI
	stackFeatures    keboola.FeaturesMap
	stackServices    keboola.ServicesMap
	storageAPIHost   string
}

type PublicScopeOption func(*publicScopeConfig)

type publicScopeConfig struct {
	preloadComponents bool
	logIndexLoading   bool
}

func newPublicScopeConfig(opts []PublicScopeOption) publicScopeConfig {
	cfg := publicScopeConfig{preloadComponents: false}
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

// WithPreloadComponents defines if the components should be retrieved from Storage index on startup.
func WithPreloadComponents(v bool) PublicScopeOption {
	return func(c *publicScopeConfig) {
		c.preloadComponents = v
	}
}

// WithLogIndexLoading enables logging of index loading and also the number of loaded components.
func WithLogIndexLoading(v bool) PublicScopeOption {
	return func(c *publicScopeConfig) {
		c.logIndexLoading = v
	}
}

func NewPublicScope(ctx context.Context, baseScp BaseScope, storageAPIHost string, opts ...PublicScopeOption) (v PublicScope, err error) {
	return newPublicScope(ctx, baseScp, storageAPIHost, opts...)
}

func newPublicScope(ctx context.Context, baseScp BaseScope, storageAPIHost string, opts ...PublicScopeOption) (v *publicScope, err error) {
	ctx, span := baseScp.Telemetry().Tracer().Start(ctx, "keboola.go.common.dependencies.NewPublicScope")
	defer span.End(&err)

	cfg := newPublicScopeConfig(opts)
	v = &publicScope{base: baseScp, storageAPIHost: storageAPIHost}
	baseHTTPClient := baseScp.HTTPClient()

	// Optionally log index loading
	var logger log.Logger
	if cfg.logIndexLoading {
		logger = baseScp.Logger()
	} else {
		logger = log.NewNopLogger()
	}

	// Load API index
	startTime := time.Now()
	var index *keboola.Index
	var indexWithComponents *keboola.IndexComponents
	if cfg.preloadComponents {
		logger.Info(ctx, "loading Storage API index with components")
		indexWithComponents, err = keboola.APIIndexWithComponents(ctx, storageAPIHost, keboola.WithClient(&baseHTTPClient), keboola.WithOnSuccessTimeout(1*time.Minute))
		if err != nil {
			return nil, err
		}
		index = &indexWithComponents.Index
		logger.WithDuration(time.Since(startTime)).Infof(ctx, `loaded Storage API index with "%d" components`, len(indexWithComponents.Components))
	} else {
		logger.Info(ctx, "loading Storage API index without components")
		index, err = keboola.APIIndex(ctx, storageAPIHost, keboola.WithClient(&baseHTTPClient), keboola.WithOnSuccessTimeout(1*time.Minute))
		if err != nil {
			return nil, err
		}
		logger.WithDuration(time.Since(startTime)).Infof(ctx, "loaded Storage API index without component")
	}

	// Create API
	v.keboolaPublicAPI = keboola.NewPublicAPIFromIndex(storageAPIHost, index, keboola.WithClient(&baseHTTPClient), keboola.WithOnSuccessTimeout(1*time.Minute))

	// Cache components list if it has been loaded
	if indexWithComponents != nil {
		v.components.Set(model.NewComponentsProvider(indexWithComponents, v.base.Logger(), v.keboolaPublicAPI))
	}

	// Set values derived from the index
	v.stackFeatures = index.Features.ToMap()
	v.stackServices = index.Services.ToMap()

	return v, nil
}

func storageAPIIndexWithComponents(ctx context.Context, d BaseScope, keboolaPublicAPI *keboola.PublicAPI) (index *keboola.IndexComponents, err error) {
	startTime := time.Now()
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.common.dependencies.public.storageApiIndexWithComponents")
	defer span.End(&err)

	index, err = keboolaPublicAPI.IndexComponentsRequest().Send(ctx)
	if err != nil {
		return nil, err
	}
	d.Logger().WithDuration(time.Since(startTime)).Debugf(ctx, "Storage API index with components loaded")
	return index, nil
}

func (v *publicScope) check() {
	if v == nil {
		panic(errors.New("dependencies public scope is not initialized"))
	}
}

func (v *publicScope) StorageAPIHost() string {
	v.check()
	return v.storageAPIHost
}

func (v *publicScope) KeboolaPublicAPI() *keboola.PublicAPI {
	v.check()
	return v.keboolaPublicAPI
}

func (v *publicScope) StackFeatures() keboola.FeaturesMap {
	v.check()
	return v.stackFeatures
}

func (v *publicScope) StackServices() keboola.ServicesMap {
	v.check()
	return v.stackServices
}

func (v *publicScope) Components() *model.ComponentsMap {
	v.check()
	return v.ComponentsProvider().Components()
}

func (v *publicScope) ComponentsProvider() *model.ComponentsProvider {
	v.check()
	return v.components.MustInitAndGet(func() *model.ComponentsProvider {
		index, err := storageAPIIndexWithComponents(context.Background(), v.base, v.keboolaPublicAPI)
		if err != nil {
			panic(err)
		}
		return model.NewComponentsProvider(index, v.base.Logger(), v.KeboolaPublicAPI())
	})
}
