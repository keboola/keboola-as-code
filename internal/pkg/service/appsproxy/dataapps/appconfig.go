package dataapps

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
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/syncmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// staleCacheFallbackDuration is the maximum duration for which the old configuration of an application is used if loading new configuration is not possible.
const staleCacheFallbackDuration = time.Hour

// notificationInterval sets how often the proxy sends notifications to sandboxes service.
// If the last notification for given app was less than this interval ago then the notification is skipped.
const notificationInterval = time.Second * 30

type Client interface {
	Notify(ctx context.Context, appID string) error
	Wakeup(ctx context.Context, appID string) error
	LoadConfig(ctx context.Context, appID string) (AppProxyConfig, bool, error)
}

type sandboxesServiceClient struct {
	logger        log.Logger
	clock         clock.Clock
	sender        request.Sender
	cache         *syncmap.SyncMap[string, cacheItem]
	notifications *syncmap.SyncMap[string, notificationItem]
}

type cacheItem struct {
	config     AppProxyConfig
	eTag       string
	expiresAt  time.Time
	updateLock *sync.Mutex
}

type notificationItem struct {
	nextNotificationAfter time.Time
	updateLock            *sync.Mutex
}

func NewSandboxesServiceLoader(logger log.Logger, clock clock.Clock, client client.Client, baseURL string, token string) Client {
	return &sandboxesServiceClient{
		logger: logger,
		clock:  clock,
		sender: client.WithBaseURL(baseURL).WithHeader("X-KBC-ManageApiToken", token),
		cache: syncmap.New[string, cacheItem](func() *cacheItem {
			return &cacheItem{
				updateLock: &sync.Mutex{},
			}
		}),
		notifications: syncmap.New[string, notificationItem](func() *notificationItem {
			return &notificationItem{
				updateLock: &sync.Mutex{},
			}
		}),
	}
}

func (l *sandboxesServiceClient) Notify(ctx context.Context, appID string) error {
	// Get cache item or init an empty item
	item := l.notifications.GetOrInit(appID)

	// Only one notification runs in parallel.
	// If there is an in-flight update, we are waiting for its results.
	item.updateLock.Lock()
	defer item.updateLock.Unlock()

	// Return config from cache if still valid
	now := l.clock.Now()

	if now.Before(item.nextNotificationAfter) {
		// Skip if a notification was sent less than notificationInterval ago
		return nil
	}

	// Update nextNotificationAfter time
	item.nextNotificationAfter = now.Add(notificationInterval)

	// Send the notification
	_, err := NotifyAppUsage(l.sender, appID, now).Send(ctx)
	if err != nil {
		l.logger.Errorf(ctx, `Failed notifying Sandboxes Service about a request to app "%s": %s`, appID, err.Error())

		return err
	}

	return nil
}

func (l *sandboxesServiceClient) Wakeup(ctx context.Context, appID string) error {
	_, err := WakeupApp(l.sender, appID).Send(ctx)
	if err != nil {
		l.logger.Errorf(ctx, `Failed sending wakeup request to Sandboxes Service about for app "%s": %s`, appID, err.Error())

		return err
	}

	return nil
}

// LoadConfig gets the current configuration from Sandboxes Service.
// It handles local caching based on the Cache-Control and ETag headers.
func (l *sandboxesServiceClient) LoadConfig(ctx context.Context, appID string) (out AppProxyConfig, modified bool, err error) {
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

func (l *sandboxesServiceClient) handleError(ctx context.Context, appID string, now time.Time, err error, fallbackItem *cacheItem) (AppProxyConfig, error) {
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
	// If expireAt is zero then there is no cached value.
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
