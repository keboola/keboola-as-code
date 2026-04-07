// Package jobtriggersink implements a sink type that triggers a Keboola Queue job
// on every received record. No local storage is used; the job is fired directly
// via the Queue API in WriteRecord.
package jobtriggersink

import (
	"context"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
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
// Returns ("", false) when no token is present (e.g. background activation).
type TokenFromContext func(ctx context.Context) (string, bool)

// storedToken is the value stored in etcd under the token schema prefix.
type storedToken struct {
	// Token is the raw SAPI token string used to authenticate job trigger requests.
	Token string `json:"token"`
}

// tokenSchema is the etcd prefix for per-sink SAPI tokens.
type tokenSchema struct {
	etcdop.PrefixT[storedToken]
}

func newTokenSchema(s *serde.Serde) tokenSchema {
	return tokenSchema{PrefixT: etcdop.NewTypedPrefix[storedToken]("stream/job-trigger/token", s)}
}

func (v tokenSchema) forSink(k key.SinkKey) etcdop.KeyT[storedToken] {
	return v.Key(k.String())
}

// Bridge manages the token lifecycle and statistics for job trigger sinks.
// It stores the project's SAPI token in etcd at sink activation and removes it on deactivation.
// It also tracks triggered/failed job counts in etcd for each sink.
type Bridge struct {
	logger           log.Logger
	client           etcd.KV
	schema           tokenSchema
	statsSchema      statsSchema
	publicAPI        *keboola.PublicAPI
	tokenFromContext TokenFromContext
}

type bridgeDeps interface {
	Logger() log.Logger
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin.Plugins
	KeboolaPublicAPI() *keboola.PublicAPI
}

// NewBridge creates a Bridge and registers plugin lifecycle hooks for job trigger sinks.
// tokenFromContext is called during sink activation to extract the SAPI token from the request context.
func NewBridge(d bridgeDeps, tokenFromContext TokenFromContext) *Bridge {
	b := &Bridge{
		logger:           d.Logger().WithComponent("job-trigger.bridge"),
		client:           d.EtcdClient(),
		schema:           newTokenSchema(d.EtcdSerde()),
		statsSchema:      newStatsSchema(d.EtcdSerde()),
		publicAPI:        d.KeboolaPublicAPI(),
		tokenFromContext: tokenFromContext,
	}
	b.registerPlugins(d.Plugins())
	return b
}

func (b *Bridge) registerPlugins(plugins *plugin.Plugins) {
	// Store token when a job trigger sink becomes active (created or re-enabled).
	plugins.Collection().OnSinkActivation(func(ctx context.Context, now time.Time, by definition.By, original, sink *definition.Sink) error {
		if sink.Type != definition.SinkTypeJobTrigger {
			return nil
		}
		return b.storeToken(ctx, sink.SinkKey)
	})

	// Delete stored token when a job trigger sink is deactivated (deleted or disabled).
	plugins.Collection().OnSinkDeactivation(func(ctx context.Context, now time.Time, by definition.By, original, sink *definition.Sink) error {
		if sink.Type != definition.SinkTypeJobTrigger {
			return nil
		}
		return b.deleteToken(ctx, sink.SinkKey)
	})
}

// storeToken reads the project's SAPI token from context and persists it in etcd.
// It is called during sink activation (create/re-enable) via the API, so the token
// is always present in the context at that point.
func (b *Bridge) storeToken(ctx context.Context, sinkKey key.SinkKey) error {
	token, ok := b.tokenFromContext(ctx)
	if !ok || token == "" {
		// Not in an API request context — sink was activated by a background process.
		// The token should already be in etcd from a prior activation; skip silently.
		b.logger.Debugf(ctx, "no token in context for sink %s, skipping token storage", sinkKey)
		return nil
	}

	b.logger.Debugf(ctx, "storing SAPI token for job trigger sink %s", sinkKey)
	stored := storedToken{Token: token}
	if err := b.schema.forSink(sinkKey).Put(b.client, stored).Do(ctx).Err(); err != nil {
		return errors.Errorf("cannot store token for job trigger sink %s: %w", sinkKey, err)
	}
	return nil
}

// deleteToken removes the stored token from etcd on sink deactivation.
func (b *Bridge) deleteToken(ctx context.Context, sinkKey key.SinkKey) error {
	b.logger.Debugf(ctx, "deleting SAPI token for job trigger sink %s", sinkKey)
	if err := b.schema.forSink(sinkKey).Delete(b.client).Do(ctx).Err(); err != nil {
		return errors.Errorf("cannot delete token for job trigger sink %s: %w", sinkKey, err)
	}
	return nil
}

// APIForSink loads the stored token from etcd and returns an AuthorizedAPI for the sink.
func (b *Bridge) APIForSink(ctx context.Context, sinkKey key.SinkKey) (*keboola.AuthorizedAPI, error) {
	stored, err := b.schema.forSink(sinkKey).GetOrErr(b.client).Do(ctx).ResultOrErr()
	if err != nil {
		return nil, errors.Errorf("cannot load token for job trigger sink %s: %w", sinkKey, err)
	}
	if stored.Token == "" {
		return nil, errors.Errorf("empty token stored for job trigger sink %s", sinkKey)
	}
	return b.publicAPI.NewAuthorizedAPI(stored.Token, 3*time.Minute), nil
}

// AddStats merges the given deltas into the persisted stats for a job trigger sink.
// It uses a simple read-modify-write. Occasional concurrent writes from multiple source nodes
// are acceptable — stats are approximate counters for user visibility.
func (b *Bridge) AddStats(ctx context.Context, sinkKey key.SinkKey, triggered, failed uint64, firstAt, lastAt utctime.UTCTime) error {
	statsKey := b.statsSchema.forSink(sinkKey)

	current, err := statsKey.GetOrEmpty(b.client).Do(ctx).ResultOrErr()
	if err != nil {
		return errors.Errorf("cannot read stats for job trigger sink %s: %w", sinkKey, err)
	}

	current.TriggeredCount += triggered
	current.FailedCount += failed
	if !firstAt.IsZero() {
		if current.FirstTriggeredAt.IsZero() || current.FirstTriggeredAt.After(firstAt) {
			current.FirstTriggeredAt = firstAt
		}
		if lastAt.After(current.LastTriggeredAt) {
			current.LastTriggeredAt = lastAt
		}
	}

	if err := statsKey.Put(b.client, current).Do(ctx).Err(); err != nil {
		return errors.Errorf("cannot save stats for job trigger sink %s: %w", sinkKey, err)
	}
	return nil
}

// Stats returns the current statistics for a job trigger sink.
func (b *Bridge) Stats(ctx context.Context, sinkKey key.SinkKey) (SinkStats, error) {
	stats, err := b.statsSchema.forSink(sinkKey).GetOrEmpty(b.client).Do(ctx).ResultOrErr()
	if err != nil {
		return SinkStats{}, errors.Errorf("cannot read stats for job trigger sink %s: %w", sinkKey, err)
	}
	return stats, nil
}
