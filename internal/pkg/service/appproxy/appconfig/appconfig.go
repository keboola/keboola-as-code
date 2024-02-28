package appconfig

import (
	"context"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/request"
)

type Loader struct {
	clock  clock.Clock
	sender request.Sender
	cache  map[string]cacheItem
}

type cacheItem struct {
	config    AppProxyConfig
	eTag      string
	expiresAt time.Time
}

func NewLoader(clock clock.Clock, baseURL string) *Loader {
	return &Loader{
		clock:  clock,
		sender: client.New().WithBaseURL(baseURL),
		cache:  make(map[string]cacheItem),
	}
}

func (l *Loader) LoadConfig(ctx context.Context, appID string) (AppProxyConfig, error) {
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
			return AppProxyConfig{}, err
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
			return AppProxyConfig{}, err
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
