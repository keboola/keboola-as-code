package appconfig

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/request"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// staleCacheFallbackDuration is the maximum duration for which the old configuration of an application is used if loading new configuration is not possible.
const staleCacheFallbackDuration = time.Hour

type Loader interface {
	LoadConfig(ctx context.Context, appID string) (AppProxyConfig, bool, error)
}

type sandboxesAPILoader struct {
	logger log.Logger
	clock  clock.Clock
	sender request.Sender
	cache  *SafeMap[string, cacheItem]
}

type cacheItem struct {
	config     AppProxyConfig
	eTag       string
	expiresAt  time.Time
	updateLock *sync.Mutex
}

func NewSandboxesAPILoader(logger log.Logger, clock clock.Clock, client client.Client, baseURL string, token string) Loader {
	return &sandboxesAPILoader{
		logger: logger,
		clock:  clock,
		sender: client.WithBaseURL(baseURL).WithHeader("X-KBC-ManageApiToken", token),
		cache: NewSafeMap[string, cacheItem](func() *cacheItem {
			return &cacheItem{
				updateLock: &sync.Mutex{},
			}
		}),
	}
}

func (l *sandboxesAPILoader) LoadConfig(ctx context.Context, appID string) (out AppProxyConfig, modified bool, err error) {
	// Get cache item or init an empty item
	item := l.cache.GetOrInit(appID)

	// Only one update runs in parallel.
	// If there is an in-flight update, we are waiting for its results.
	item.updateLock.Lock()
	defer item.updateLock.Unlock()

	// Return config from cache if still valid
	now := l.clock.Now()
	if now.Before(item.expiresAt) {
		return item.config, false, nil
	}

	// API request with cached eTag
	config, err := GetAppProxyConfig(l.sender, appID, item.eTag).Send(ctx)
	if err != nil {
		config, err := l.handleError(ctx, appID, now, err, item)

		return config, false, err
	}

	// Update expiration
	item.expiresAt = now.Add(minDuration(config.maxAge, time.Hour))

	// Update item if needed
	modified = config.eTag == "" || config.eTag != item.eTag
	if modified {
		item.config = *config
		item.eTag = config.eTag
	}

	return item.config, modified, nil
}

func (l *sandboxesAPILoader) handleError(ctx context.Context, appID string, now time.Time, err error, fallbackItem *cacheItem) (AppProxyConfig, error) {
	var sandboxesError *SandboxesError
	errors.As(err, &sandboxesError)
	if sandboxesError != nil && sandboxesError.StatusCode() == http.StatusNotFound {
		return AppProxyConfig{}, err
	}

	logger := l.logger
	if sandboxesError != nil {
		logger = l.logger.With(attribute.String("exceptionId", sandboxesError.ExceptionID))
	}

	// An error other than 404 is considered a temporary failure. Keep using the stale cache for staleCacheFallbackDuration as fallback.
	if !fallbackItem.expiresAt.IsZero() && now.Before(fallbackItem.expiresAt.Add(staleCacheFallbackDuration)) {
		logger.Warnf(ctx, `Using stale cache for app "%s": %s`, appID, err.Error())

		return fallbackItem.config, nil
	}

	logger.Errorf(ctx, `Failed loading config for app "%s": %s`, appID, err.Error())

	return AppProxyConfig{}, err
}

func minDuration(durationA time.Duration, durationB time.Duration) time.Duration {
	if durationA <= durationB {
		return durationA
	}
	return durationB
}
