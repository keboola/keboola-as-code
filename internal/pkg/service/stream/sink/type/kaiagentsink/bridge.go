// Package kaiagentsink implements a sink type that forwards each received record
// to kai-agent.keboola.com via HTTP POST. Two modes are supported:
//   - chat: POSTs the record as a user message to POST /api/chat (fire-and-forget SSE).
//   - suggestions: POSTs the record to POST /api/suggestions and reads the JSON response.
package kaiagentsink

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/keboola/go-cloud-encrypt/pkg/cloudencrypt"
	"github.com/keboola/keboola-sdk-go/v2/pkg/client"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// TokenFromContext extracts the raw SAPI token string from a request context.
type TokenFromContext func(ctx context.Context) (string, bool)

type storedToken struct {
	Token          string `json:"token,omitempty"`
	EncryptedToken string `json:"encryptedToken,omitempty"`
}

type tokenSchema struct {
	etcdop.PrefixT[storedToken]
}

func newTokenSchema(s *serde.Serde) tokenSchema {
	return tokenSchema{PrefixT: etcdop.NewTypedPrefix[storedToken]("stream/kai-agent/token", s)}
}

func (v tokenSchema) forSink(k key.SinkKey) etcdop.KeyT[storedToken] {
	return v.Key(k.String())
}

// Bridge manages the SAPI token lifecycle and statistics for kaiAgent sinks.
type Bridge struct {
	logger           log.Logger
	client           etcd.KV
	schema           tokenSchema
	statsSchema      statsSchema
	httpClient       client.Client
	storageAPIHost   string
	tokenFromContext TokenFromContext
	tokenEncryptor   *cloudencrypt.GenericEncryptor[string]
}

type bridgeDeps interface {
	Logger() log.Logger
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin.Plugins
	HTTPClient() client.Client
	StorageAPIHost() string
	Encryptor() cloudencrypt.Encryptor
}

func NewBridge(d bridgeDeps, tokenFromContext TokenFromContext) *Bridge {
	b := &Bridge{
		logger:           d.Logger().WithComponent("kai-agent.bridge"),
		client:           d.EtcdClient(),
		schema:           newTokenSchema(d.EtcdSerde()),
		statsSchema:      newStatsSchema(d.EtcdSerde()),
		httpClient:       d.HTTPClient(),
		storageAPIHost:   d.StorageAPIHost(),
		tokenFromContext: tokenFromContext,
	}
	if enc := d.Encryptor(); enc != nil {
		b.tokenEncryptor = cloudencrypt.NewGenericEncryptor[string](enc)
	}
	b.registerPlugins(d.Plugins())
	return b
}

func (b *Bridge) registerPlugins(plugins *plugin.Plugins) {
	plugins.Collection().OnSinkActivation(func(ctx context.Context, now time.Time, by definition.By, original, sink *definition.Sink) error {
		if sink.Type != definition.SinkTypeKaiAgent {
			return nil
		}
		return b.storeToken(ctx, sink.SinkKey)
	})

	plugins.Collection().OnSinkDeactivation(func(ctx context.Context, now time.Time, by definition.By, original, sink *definition.Sink) error {
		if sink.Type != definition.SinkTypeKaiAgent {
			return nil
		}
		if err := b.deleteToken(ctx, sink.SinkKey); err != nil {
			return err
		}
		return b.deleteStats(ctx, sink.SinkKey)
	})
}

func (b *Bridge) storeToken(ctx context.Context, sinkKey key.SinkKey) error {
	token, ok := b.tokenFromContext(ctx)
	if !ok || token == "" {
		b.logger.Debugf(ctx, "no token in context for sink %s, skipping token storage", sinkKey)
		return nil
	}

	b.logger.Debugf(ctx, "storing SAPI token for kai-agent sink %s", sinkKey)
	stored := storedToken{}
	if b.tokenEncryptor != nil {
		metadata := cloudencrypt.Metadata{"sink": sinkKey.String()}
		ciphertext, err := b.tokenEncryptor.Encrypt(ctx, token, metadata)
		if err != nil {
			return errors.Errorf("cannot encrypt token for kai-agent sink %s: %w", sinkKey, err)
		}
		stored.EncryptedToken = string(ciphertext)
	} else {
		stored.Token = token
	}
	if err := b.schema.forSink(sinkKey).Put(b.client, stored).Do(ctx).Err(); err != nil {
		return errors.Errorf("cannot store token for kai-agent sink %s: %w", sinkKey, err)
	}
	return nil
}

func (b *Bridge) deleteToken(ctx context.Context, sinkKey key.SinkKey) error {
	b.logger.Debugf(ctx, "deleting SAPI token for kai-agent sink %s", sinkKey)
	if err := b.schema.forSink(sinkKey).Delete(b.client).Do(ctx).Err(); err != nil {
		return errors.Errorf("cannot delete token for kai-agent sink %s: %w", sinkKey, err)
	}
	return nil
}

func (b *Bridge) deleteStats(ctx context.Context, sinkKey key.SinkKey) error {
	b.logger.Debugf(ctx, "deleting stats for kai-agent sink %s", sinkKey)
	if err := b.statsSchema.forSink(sinkKey).Delete(b.client).Do(ctx).Err(); err != nil {
		return errors.Errorf("cannot delete stats for kai-agent sink %s: %w", sinkKey, err)
	}
	return nil
}

// TokenForSink loads and decrypts the stored SAPI token for a sink.
func (b *Bridge) TokenForSink(ctx context.Context, sinkKey key.SinkKey) (string, error) {
	stored, err := b.schema.forSink(sinkKey).GetOrErr(b.client).Do(ctx).ResultOrErr()
	if err != nil {
		return "", errors.Errorf("cannot load token for kai-agent sink %s: %w", sinkKey, err)
	}

	if stored.EncryptedToken != "" {
		if b.tokenEncryptor == nil {
			return "", errors.Errorf("token for kai-agent sink %s is encrypted but no encryptor is configured", sinkKey)
		}
		metadata := cloudencrypt.Metadata{"sink": sinkKey.String()}
		raw, decErr := b.tokenEncryptor.Decrypt(ctx, []byte(stored.EncryptedToken), metadata)
		if decErr != nil {
			return "", errors.Errorf("cannot decrypt token for kai-agent sink %s: %w", sinkKey, decErr)
		}
		return raw, nil
	}

	if stored.Token == "" {
		return "", errors.Errorf("empty token stored for kai-agent sink %s", sinkKey)
	}
	return stored.Token, nil
}

// BaseURL returns the kai-agent base URL derived from the Storage API host.
func (b *Bridge) BaseURL() string {
	return KaiAgentBaseURL(b.storageAPIHost)
}

// KaiAgentBaseURL derives the kai-agent base URL from a Storage API host.
// e.g. "connection.keboola.com" → "https://kai-agent.keboola.com"
func KaiAgentBaseURL(storageAPIHost string) string {
	suffix := strings.TrimPrefix(storageAPIHost, "connection.")
	return fmt.Sprintf("https://kai-agent.%s", suffix)
}

// HTTPClient returns the shared HTTP client for making kai-agent requests.
func (b *Bridge) HTTPClient() client.Client {
	return b.httpClient
}

// AddStats merges deltas into persisted stats for a kai-agent sink.
func (b *Bridge) AddStats(ctx context.Context, sinkKey key.SinkKey, sent, failed uint64, firstAt, lastAt utctime.UTCTime) error {
	statsKey := b.statsSchema.forSink(sinkKey)

	current, err := statsKey.GetOrEmpty(b.client).Do(ctx).ResultOrErr()
	if err != nil {
		return errors.Errorf("cannot read stats for kai-agent sink %s: %w", sinkKey, err)
	}

	current.SentCount += sent
	current.FailedCount += failed
	if !firstAt.IsZero() {
		if current.FirstSentAt.IsZero() || current.FirstSentAt.After(firstAt) {
			current.FirstSentAt = firstAt
		}
		if lastAt.After(current.LastSentAt) {
			current.LastSentAt = lastAt
		}
	}

	if err := statsKey.Put(b.client, current).Do(ctx).Err(); err != nil {
		return errors.Errorf("cannot save stats for kai-agent sink %s: %w", sinkKey, err)
	}
	return nil
}

// Stats returns persisted statistics for a kai-agent sink.
func (b *Bridge) Stats(ctx context.Context, sinkKey key.SinkKey) (SinkStats, error) {
	stats, err := b.statsSchema.forSink(sinkKey).GetOrEmpty(b.client).Do(ctx).ResultOrErr()
	if err != nil {
		return SinkStats{}, errors.Errorf("cannot read stats for kai-agent sink %s: %w", sinkKey, err)
	}
	return stats, nil
}
