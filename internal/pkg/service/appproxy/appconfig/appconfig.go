package appconfig

import (
	"context"
	"net/http"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/request"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Loader interface {
	LoadConfig(ctx context.Context, appID string) (AppProxyConfig, error)
}

type sandboxesAPILoader struct {
	logger log.Logger
	clock  clock.Clock
	sender request.Sender
	cache  map[string]cacheItem
}

type cacheItem struct {
	config    AppProxyConfig
	eTag      string
	expiresAt time.Time
}

func NewSandboxesAPILoader(logger log.Logger, clock clock.Clock, baseURL string, token string) Loader {
	return &sandboxesAPILoader{
		logger: logger,
		clock:  clock,
		sender: client.New().WithBaseURL(baseURL).WithHeader("X-KBC-ManageApiToken", token),
		cache:  make(map[string]cacheItem),
	}
}

const staleCacheFallbackDuration = time.Hour

func (l *sandboxesAPILoader) LoadConfig(ctx context.Context, appID string) (AppProxyConfig, error) {
	var config *AppProxyConfig
	var err error
	now := l.clock.Now()

	if item, ok := l.cache[appID]; ok {
		// Return config from cache if still valid
		if now.Before(item.expiresAt) {
			return item.config, nil
		}

		// API request with cached ETag
		config, err = GetAppProxyConfig(l.sender, appID, item.eTag).Send(ctx)
		if err != nil {
			return l.handleError(ctx, appID, now, err, &item)
		}

		// Update expiration and use the cached config if ETag is still the same
		if config.eTag == item.eTag {
			l.cache[appID] = cacheItem{
				config:    item.config,
				eTag:      item.eTag,
				expiresAt: now.Add(config.maxAge),
			}
			return item.config, nil
		}
	} else {
		// API request without ETag because cache is empty
		config, err = GetAppProxyConfig(l.sender, appID, "").Send(ctx)
		if err != nil {
			return l.handleError(ctx, appID, now, err, nil)
		}
	}

	// Save result to cache
	l.cache[appID] = cacheItem{
		config:    *config,
		eTag:      config.eTag,
		expiresAt: now.Add(config.maxAge),
	}
	return *config, nil
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
	if fallbackItem != nil && now.Before(fallbackItem.expiresAt.Add(staleCacheFallbackDuration)) {
		logger.Warnf(ctx, `Using stale cache for app "%s": %s`, appID, err.Error())

		return fallbackItem.config, nil
	}

	logger.Errorf(ctx, `Failed loading config for app "%s": %s`, appID, err.Error())

	return AppProxyConfig{}, err
}
