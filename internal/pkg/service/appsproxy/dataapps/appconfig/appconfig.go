// Package appconfig provides application configuration loading with cache and expiration handling.
package appconfig

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/syncmap"
	svcErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// staleCacheFallbackDuration is the maximum duration for which the old configuration of an application is used if loading new configuration is not possible.
const staleCacheFallbackDuration = time.Hour

type Loader struct {
	clock     clockwork.Clock
	logger    log.Logger
	telemetry telemetry.Telemetry
	api       *api.API
	cache     *syncmap.SyncMap[api.AppID, cachedAppProxyConfig]
}

type cachedAppProxyConfig struct {
	lock      *sync.Mutex
	config    api.AppConfig
	expiresAt time.Time
}

type dependencies interface {
	Clock() clockwork.Clock
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	AppsAPI() *api.API
}

func NewLoader(d dependencies) *Loader {
	return &Loader{
		clock:     d.Clock(),
		logger:    d.Logger(),
		api:       d.AppsAPI(),
		telemetry: d.Telemetry(),
		cache: syncmap.New[api.AppID, cachedAppProxyConfig](func(api.AppID) *cachedAppProxyConfig {
			return &cachedAppProxyConfig{lock: &sync.Mutex{}}
		}),
	}
}

// GetConfig gets the AppConfig by the ID from Sandboxes Service.
// It handles local caching based on the Cache-Control and ETag headers.
func (l *Loader) GetConfig(ctx context.Context, appID api.AppID) (out api.AppConfig, modified bool, err error) {
	ctx, span := l.telemetry.Tracer().Start(ctx, "keboola.go.apps-proxy.appconfig.Loader.GetConfig")
	defer span.End(&err)

	// Get cache item or init an empty item
	item := l.cache.GetOrInit(appID)

	// Only one update runs in parallel.
	// If there is an in-flight update, we are waiting for its results.
	item.lock.Lock()
	defer item.lock.Unlock()

	// Return config from cache if it is still valid.
	// At first, the item.expiresAt is zero, so the condition is skipped.
	now := l.clock.Now()
	if now.Before(item.expiresAt) {
		return item.config, false, nil
	}

	// Send API request with cached eTag.
	// At first, the item.config.ETag() is empty string.
	newConfig, err := l.api.GetAppConfig(appID, item.config.ETag()).Send(ctx)
	if err != nil {
		// The config hasn't been modified, extend expiration, return cached version
		notModifierErr := api.NotModifiedError{}
		if errors.As(err, &notModifierErr) {
			item.ExtendExpiration(now, notModifierErr.MaxAge)
			return item.config, false, nil
		}

		// Only the not found error is expected
		var apiErr *api.Error
		if errors.As(err, &apiErr) && apiErr.StatusCode() != http.StatusNotFound {
			// Log other errors
			l.logger.Errorf(ctx, `unable to load configuration for application "%s": %s`, appID, err.Error())

			// Keep the proxy running for a limited time in case of an API outage.
			// The item.expiresAt may be zero, if there is no cached version, then the condition is skipped.
			if now.Before(item.expiresAt.Add(staleCacheFallbackDuration)) {
				l.logger.Warnf(ctx, `using stale cache for app "%s": %s`, appID, err.Error())
				return item.config, false, nil
			}
		}

		// Handle not found error
		if apiErr != nil && apiErr.StatusCode() == http.StatusNotFound {
			err = svcErrors.NewResourceNotFoundError("application", appID.String(), "stack").Wrap(err)
			return api.AppConfig{}, false, err
		}

		// Return the error if:
		//  - It is not found error.
		//  - There is no cached version.
		//  - The staleCacheFallbackDuration has been exceeded.
		return api.AppConfig{}, false, svcErrors.
			NewServiceUnavailableError(errors.PrefixErrorf(err,
				`unable to load configuration for application "%s"`, appID,
			)).
			WithUserMessage(fmt.Sprintf(
				`Unable to load configuration for application "%s".`, appID,
			))
	}

	// Cache the loaded configuration
	item.config = *newConfig
	item.ExtendExpiration(now, item.config.MaxAge())
	return item.config, true, nil
}

func (v *cachedAppProxyConfig) ExtendExpiration(now time.Time, maxAge time.Duration) {
	v.expiresAt = now.Add(maxAge)
}
