package apptoken

import (
	"context"
	"time"

	"github.com/jonboulle/clockwork"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Minter struct {
	keyring *Keyring
	issuer  string
	ttl     time.Duration
	clock   clockwork.Clock
	logger  log.Logger
	apps    appStates
	// attrsDropped counts the claims of an allowlist that did not become an attribute, by reason only: a claim name is chosen
	// by the owner of the app and its value by the user, so neither is a label.
	attrsDropped metric.Int64Counter
}

type appStates interface {
	GetState(ctx context.Context, ref k8sapp.WorkloadRef) (k8sapp.AppInfo, bool)
}

type dependencies interface {
	Clock() clockwork.Clock
	Logger() log.Logger
	Config() config.Config
	AppStateWatcher() *k8sapp.StateWatcher
	Telemetry() telemetry.Telemetry
}

func NewMinter(d dependencies) (*Minter, error) {
	cfg := d.Config()
	keyring, err := NewKeyring(cfg.AppUserToken.SigningKeys, cfg.AppUserToken.ActiveKid)
	if err != nil {
		return nil, err
	}
	return &Minter{
		keyring: keyring,
		issuer:  cfg.API.PublicURL.String(),
		ttl:     cfg.AppUserToken.TTL,
		clock:   d.Clock(),
		logger:  d.Logger().WithComponent("app-user-token"),
		apps:    d.AppStateWatcher(),
		attrsDropped: d.Telemetry().Meter().IntCounter(
			"keboola.go.appsproxy.apptoken.attrs.dropped",
			"Claims of the attribute allowlist of a data app that did not become an attribute of the app user token, by reason: type (a value that cannot be one), size (too many values, too long a value) or budget (does not fit the attrs cap or the size of the token). The user then has no such attribute, which is unknown to the rules of Query Service.",
			"",
		),
	}, nil
}

// countDropped counts claims that did not become attributes. The reason is the only label.
func (m *Minter) countDropped(ctx context.Context, reason attrDrop, n int) {
	if n > 0 {
		m.attrsDropped.Add(ctx, int64(n), metric.WithAttributes(attribute.String("reason", string(reason))))
	}
}

func (m *Minter) JWKS() []byte {
	return m.keyring.JWKS()
}
